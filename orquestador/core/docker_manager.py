import os
import re
import json
import shutil
import socket
import subprocess
import time


# ── Plantillas de docker-compose ──────────────────────────────────────────────
# Cada plantilla genera un docker-compose.yml mínimo para la BD detectada.
# Los campos {port}, {user}, {password}, {database} se reemplazan en tiempo de ejecución.

_COMPOSE_TEMPLATES = {
    "postgresql": """\
version: '3.8'
services:
  db:
    image: postgres:16
    ports:
      - "{port}:5432"
    environment:
      POSTGRES_USER: {user}
      POSTGRES_PASSWORD: {password}
      POSTGRES_DB: {database}
    restart: unless-stopped
""",
    "mysql": """\
version: '3.8'
services:
  db:
    image: mysql:8
    ports:
      - "{port}:3306"
    environment:
      MYSQL_USER: {user}
      MYSQL_PASSWORD: {password}
      MYSQL_ROOT_PASSWORD: {password}
      MYSQL_DATABASE: {database}
    restart: unless-stopped
""",
    "mongodb": """\
version: '3.8'
services:
  db:
    image: mongo:7
    ports:
      - "{port}:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: {user}
      MONGO_INITDB_ROOT_PASSWORD: {password}
      MONGO_INITDB_DATABASE: {database}
    restart: unless-stopped
""",
    "redis": """\
version: '3.8'
services:
  db:
    image: redis:7
    ports:
      - "{port}:6379"
    command: {redis_cmd}
    restart: unless-stopped
""",
}

# Puerto estándar de cada BD (dentro del contenedor)
_DB_DEFAULT_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mongodb":    27017,
    "redis":      6379,
}

# Puerto del contenedor para cada motor (usado para mapear host→contenedor)
_DB_CONTAINER_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mariadb":    3306,
    "mongodb":    27017,
    "redis":      6379,
}

# Variables de entorno que cada BD usa para user/password/database en docker-compose
_DB_ENV_FIELDS = {
    "postgresql": {"user": ["POSTGRES_USER"], "password": ["POSTGRES_PASSWORD"], "database": ["POSTGRES_DB"]},
    "mysql":      {"user": ["MYSQL_USER", "MARIADB_USER"], "password": ["MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD"], "database": ["MYSQL_DATABASE", "MARIADB_DATABASE"]},
    "mariadb":    {"user": ["MARIADB_USER", "MYSQL_USER"], "password": ["MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD", "MYSQL_PASSWORD"], "database": ["MARIADB_DATABASE", "MYSQL_DATABASE"]},
    "mongodb":    {"user": ["MONGO_INITDB_ROOT_USERNAME"], "password": ["MONGO_INITDB_ROOT_PASSWORD"], "database": ["MONGO_INITDB_DATABASE"]},
    "redis":      {"password": ["REDIS_PASSWORD", "REQUIREPASS"]},
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _find_free_port() -> int:
    # Pedir al SO un puerto libre (bind en 0 → el SO asigna uno disponible)
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _port_in_use(port: int) -> bool:
    # Intentar conectar al puerto: si hay respuesta → está ocupado
    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False


def _patch_compose_ports(compose_file: str) -> tuple[str, dict]:
    """
    Lee el compose.yml y reemplaza cualquier puerto host que esté ocupado
    por un puerto libre. Devuelve (contenido_parcheado, mapa_de_cambios).
    """
    with open(compose_file, encoding="utf-8", errors="replace") as f:
        content = f.read()
    # Buscar todos los mapeos host:contenedor (ej. "5432:5432" o '5432:5432')
    pattern = re.compile(r'(["\']?)(\d+)(:)(\d+)(["\']?)')
    port_remaps = {}
    for m in pattern.finditer(content):
        host_port = m.group(2)
        if host_port in port_remaps:
            continue
        if _port_in_use(int(host_port)):
            # Puerto ocupado → buscar uno libre y anotar el cambio
            new_port = str(_find_free_port())
            port_remaps[host_port] = new_port
    if not port_remaps:
        return content, {}

    def replace_port(m):
        host_port = m.group(2)
        new_port  = port_remaps.get(host_port, host_port)
        return m.group(1) + new_port + m.group(3) + m.group(4) + m.group(5)

    return pattern.sub(replace_port, content), port_remaps


def _get_compose_running_ports(compose_file: str) -> dict:
    """
    Consulta a Docker qué puertos están publicados actualmente.
    Devuelve {puerto_contenedor: puerto_host}.
    """
    try:
        r = subprocess.run(
            ["docker", "compose", "-f", compose_file, "ps", "--format", "json"],
            capture_output=True, encoding="utf-8", errors="replace", timeout=10
        )
        if r.returncode != 0 or not r.stdout.strip():
            return {}
        ports = {}
        for line in r.stdout.strip().splitlines():
            try:
                svc = json.loads(line)
                if svc.get("State") != "running":
                    continue
                for pub in svc.get("Publishers", []):
                    target    = pub.get("TargetPort")   # puerto dentro del contenedor
                    published = pub.get("PublishedPort") # puerto en el host
                    if target and published:
                        ports[target] = published
            except Exception:
                pass
        return ports
    except Exception:
        return {}


def _extract_compose_db_info(compose_file: str, db_type: str) -> dict:
    """
    Parsea un docker-compose.yml con regex para extraer host_port, user, password, database.
    No usa librería YAML para evitar dependencias externas.
    """
    try:
        with open(compose_file, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception:
        return {}
    container_port = _DB_CONTAINER_PORTS.get(db_type)
    info = {}
    if container_port:
        # Buscar el mapeo host:contenedor para este tipo de BD
        m = re.search(r'["\']?(\d+):' + str(container_port) + r'["\']?', content)
        if m:
            info["host_port"] = int(m.group(1))
    # Extraer credenciales de las variables de entorno del compose
    env_map = _DB_ENV_FIELDS.get(db_type, {})
    for field, env_vars in env_map.items():
        for var in env_vars:
            m = re.search(rf'{var}\s*[=:]\s*["\']?([^"\'\s\n]+)["\']?', content)
            if m:
                info[field] = m.group(1)
                break
    return info


# ── DockerManager ──────────────────────────────────────────────────────────────
# Gestiona contenedores Docker para proyectos que tienen BD.
# Sabe crear redes, levantar/bajar compose, parchear puertos en conflicto.

class DockerManager:

    def is_docker_available(self) -> bool:
        return shutil.which("docker") is not None

    def create_network(self, project_name: str) -> bool:
        # Red Docker aislada por proyecto (orq_<nombre>_net)
        net = f"orq_{project_name}_net"
        r = subprocess.run(
            ["docker", "network", "create", net],
            capture_output=True, encoding="utf-8", errors="replace"
        )
        # returncode 1 con "already exists" es aceptable → idempotente
        return r.returncode == 0 or "already exists" in r.stderr

    def remove_network(self, project_name: str) -> bool:
        net = f"orq_{project_name}_net"
        r = subprocess.run(
            ["docker", "network", "rm", net],
            capture_output=True, encoding="utf-8", errors="replace"
        )
        return r.returncode == 0

    def up(self, project_path: str, network_name: str, port_mapping: dict) -> dict:
        if not self.is_docker_available():
            return {"ok": False, "error": f"{'docker'} no está instalado"}

        # Buscar el compose.yml en el directorio del proyecto
        compose_names = ("docker-compose.yml", "docker-compose.yaml",
                         "docker-compose.override.yml", "compose.yml", "compose.yaml")
        compose_file = None
        for name in compose_names:
            candidate = os.path.join(project_path, name)
            if os.path.isfile(candidate):
                compose_file = candidate
                break
        if not compose_file:
            return {"ok": False, "error": "No se encontró docker-compose.yml en el proyecto"}

        # Si ya hay contenedores corriendo, no parchear puertos (ya están asignados)
        running_ports = _get_compose_running_ports(compose_file)
        original_content = None
        port_remaps = {}
        if not running_ports:
            try:
                # Parchear puertos en conflicto antes de levantar
                patched_content, port_remaps = _patch_compose_ports(compose_file)
                if port_remaps:
                    # Guardar el contenido original para restaurar si falla
                    with open(compose_file, encoding="utf-8", errors="replace") as f:
                        original_content = f.read()
                    with open(compose_file, "w", encoding="utf-8") as f:
                        f.write(patched_content)
            except Exception as e:
                return {"ok": False, "error": f"Error al analizar docker-compose.yml: {e}"}

        try:
            result = subprocess.run(
                ["docker", "compose", "-f", compose_file, "up", "-d"],
                capture_output=True, encoding="utf-8", errors="replace", timeout=120,
                cwd=os.path.dirname(compose_file)
            )
            # Si falla por conflicto de nombres de contenedor → bajar y reintentar una vez
            if result.returncode != 0 and any(
                k in (result.stderr + result.stdout)
                for k in ("already in use", "Conflict", "already exists")
            ):
                subprocess.run(
                    ["docker", "compose", "-f", compose_file, "down"],
                    capture_output=True, encoding="utf-8", errors="replace", timeout=60,
                    cwd=os.path.dirname(compose_file)
                )
                result = subprocess.run(
                    ["docker", "compose", "-f", compose_file, "up", "-d"],
                    capture_output=True, encoding="utf-8", errors="replace", timeout=120,
                    cwd=os.path.dirname(compose_file)
                )
            if result.returncode != 0:
                # Restaurar compose original si el arranque falló tras parchear
                if original_content is not None:
                    with open(compose_file, "w", encoding="utf-8") as f:
                        f.write(original_content)
                return {"ok": False, "error": result.stderr.strip() or result.stdout.strip()}
            response = {"ok": True, "output": result.stdout.strip(),
                        "compose_file": compose_file, "port_remaps": port_remaps}
            return response
        except subprocess.TimeoutExpired:
            if original_content is not None:
                with open(compose_file, "w") as f:
                    f.write(original_content)
            return {"ok": False, "error": "Tiempo de espera agotado levantando los servicios"}
        except Exception as e:
            if original_content is not None:
                with open(compose_file, "w") as f:
                    f.write(original_content)
            return {"ok": False, "error": str(e)}

    def down(self, project_path: str) -> dict:
        compose_names = ("docker-compose.yml", "docker-compose.yaml",
                         "docker-compose.override.yml", "compose.yml", "compose.yaml")
        compose_file = None
        for name in compose_names:
            candidate = os.path.join(project_path, name)
            if os.path.isfile(candidate):
                compose_file = candidate
                break
        if not compose_file:
            return {"ok": True}  # No hay compose → nada que parar

        try:
            # --volumes: también elimina los volúmenes anónimos (datos de la BD)
            r = subprocess.run(
                ["docker", "compose", "-f", compose_file, "down", "--volumes"],
                capture_output=True, encoding="utf-8", errors="replace", timeout=60,
                cwd=os.path.dirname(compose_file)
            )
            if r.returncode != 0:
                return {"ok": False, "error": r.stderr.strip() or r.stdout.strip()}
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def status(self, project_path: str) -> dict:
        compose_names = ("docker-compose.yml", "docker-compose.yaml", "compose.yml")
        compose_file = None
        for name in compose_names:
            candidate = os.path.join(project_path, name)
            if os.path.isfile(candidate):
                compose_file = candidate
                break
        if not compose_file:
            return {}
        try:
            # Obtener estado de cada servicio en el compose
            r = subprocess.run(
                ["docker", "compose", "-f", compose_file, "ps", "--format", "json"],
                capture_output=True, encoding="utf-8", errors="replace", timeout=10,
                cwd=os.path.dirname(compose_file)
            )
            if r.returncode != 0 or not r.stdout.strip():
                return {}
            services = {}
            for line in r.stdout.strip().splitlines():
                try:
                    svc = json.loads(line)
                    services[svc.get("Name", "")] = svc.get("State", "")
                except Exception:
                    pass
            return services
        except Exception:
            return {}

    def generate_compose(self, project_path: str, db_type: str,
                         credentials: dict, ports: dict) -> str:
        """Genera un docker-compose.yml a partir de la plantilla para la BD detectada."""
        template = _COMPOSE_TEMPLATES.get(db_type)
        if not template or not project_path or not os.path.isdir(project_path):
            return ""

        default_port = _DB_DEFAULT_PORTS.get(db_type, 5432)
        port     = ports.get("db_port") or credentials.get("port") or default_port
        user     = credentials.get("user") or ""
        password = credentials.get("password") or ""
        database = credentials.get("database") or db_type

        if db_type == "redis":
            # Redis no usa user/password en variables de entorno → usa argumento de comando
            redis_cmd = f'"redis-server --requirepass {password}"' if password else "redis-server"
            content = template.format(port=port, redis_cmd=redis_cmd)
        elif db_type == "mongodb" and not (user and password):
            # MongoDB sin auth: no incluir MONGO_INITDB_ROOT_* para que arranque sin auth
            content = (
                "version: '3.8'\n"
                "services:\n"
                "  db:\n"
                f"    image: mongo:7\n"
                f"    ports:\n"
                f"      - \"{port}:27017\"\n"
                f"    restart: unless-stopped\n"
            )
        else:
            # Usar credenciales detectadas o defaults seguros si están vacías
            _user = user or "dbuser"
            _pass = password or "dbpass"
            content = template.format(port=port, user=_user, password=_pass, database=database)

        compose_path = os.path.join(project_path, "docker-compose.yml")
        try:
            with open(compose_path, "w", encoding="utf-8") as f:
                f.write(content)
            return compose_path
        except Exception:
            return ""

    def get_compose_db_info(self, compose_file: str, db_type: str) -> dict:
        return _extract_compose_db_info(compose_file, db_type)

    def get_running_ports(self, compose_file: str) -> dict:
        return _get_compose_running_ports(compose_file)

    def wait_for_port(self, host: str, port: int, timeout: int = 30) -> bool:
        # Intentar conectar al puerto una vez por segundo hasta timeout
        for _ in range(timeout):
            try:
                with socket.create_connection((host, int(port)), timeout=1):
                    return True
            except OSError:
                time.sleep(1)
        return False

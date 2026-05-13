import os
import re
import json
import shutil
import socket
import subprocess
import time


# ── Templates (moved from server.py) ──────────────────────────────────────────

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

_DB_DEFAULT_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mongodb":    27017,
    "redis":      6379,
}

_DB_CONTAINER_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mariadb":    3306,
    "mongodb":    27017,
    "redis":      6379,
}

_DB_ENV_FIELDS = {
    "postgresql": {"user": ["POSTGRES_USER"], "password": ["POSTGRES_PASSWORD"], "database": ["POSTGRES_DB"]},
    "mysql":      {"user": ["MYSQL_USER", "MARIADB_USER"], "password": ["MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD"], "database": ["MYSQL_DATABASE", "MARIADB_DATABASE"]},
    "mariadb":    {"user": ["MARIADB_USER", "MYSQL_USER"], "password": ["MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD", "MYSQL_PASSWORD"], "database": ["MARIADB_DATABASE", "MYSQL_DATABASE"]},
    "mongodb":    {"user": ["MONGO_INITDB_ROOT_USERNAME"], "password": ["MONGO_INITDB_ROOT_PASSWORD"], "database": ["MONGO_INITDB_DATABASE"]},
    "redis":      {"password": ["REDIS_PASSWORD", "REQUIREPASS"]},
}


# ── Helpers (moved from server.py) ────────────────────────────────────────────

def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _port_in_use(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False


def _patch_compose_ports(compose_file: str) -> tuple[str, dict]:
    with open(compose_file) as f:
        content = f.read()
    pattern = re.compile(r'(["\']?)(\d+)(:)(\d+)(["\']?)')
    port_remaps = {}
    for m in pattern.finditer(content):
        host_port = m.group(2)
        if host_port in port_remaps:
            continue
        if _port_in_use(int(host_port)):
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
                    target    = pub.get("TargetPort")
                    published = pub.get("PublishedPort")
                    if target and published:
                        ports[target] = published
            except Exception:
                pass
        return ports
    except Exception:
        return {}


def _extract_compose_db_info(compose_file: str, db_type: str) -> dict:
    try:
        with open(compose_file) as f:
            content = f.read()
    except Exception:
        return {}
    container_port = _DB_CONTAINER_PORTS.get(db_type)
    info = {}
    if container_port:
        m = re.search(r'["\']?(\d+):' + str(container_port) + r'["\']?', content)
        if m:
            info["host_port"] = int(m.group(1))
    env_map = _DB_ENV_FIELDS.get(db_type, {})
    for field, env_vars in env_map.items():
        for var in env_vars:
            m = re.search(rf'{var}\s*[=:]\s*["\']?([^"\'\s\n]+)["\']?', content)
            if m:
                info[field] = m.group(1)
                break
    return info


# ── DockerManager class ────────────────────────────────────────────────────────

class DockerManager:

    def is_docker_available(self) -> bool:
        return shutil.which("docker") is not None

    def create_network(self, project_name: str) -> bool:
        net = f"orq_{project_name}_net"
        r = subprocess.run(
            ["docker", "network", "create", net],
            capture_output=True, encoding="utf-8", errors="replace"
        )
        # returncode 1 with "already exists" is fine
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
            return {"ok": False, "error": "Docker no está instalado"}

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

        running_ports = _get_compose_running_ports(compose_file)
        original_content = None
        port_remaps = {}
        if not running_ports:
            try:
                patched_content, port_remaps = _patch_compose_ports(compose_file)
                if port_remaps:
                    with open(compose_file) as f:
                        original_content = f.read()
                    with open(compose_file, "w") as f:
                        f.write(patched_content)
            except Exception as e:
                return {"ok": False, "error": f"Error al analizar docker-compose.yml: {e}"}

        try:
            result = subprocess.run(
                ["docker", "compose", "-f", compose_file, "up", "-d"],
                capture_output=True, encoding="utf-8", errors="replace", timeout=120,
                cwd=os.path.dirname(compose_file)
            )
            # Container conflict: already exists from previous up attempt → down + retry
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
                if original_content is not None:
                    with open(compose_file, "w") as f:
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
            return {"ok": True}  # nothing to stop

        try:
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
        template = _COMPOSE_TEMPLATES.get(db_type)
        if not template or not project_path or not os.path.isdir(project_path):
            return ""

        default_port = _DB_DEFAULT_PORTS.get(db_type, 5432)
        port     = ports.get("db_port") or credentials.get("port") or default_port
        user     = credentials.get("user") or ""
        password = credentials.get("password") or ""
        database = credentials.get("database") or db_type

        if db_type == "redis":
            redis_cmd = f'"redis-server --requirepass {password}"' if password else "redis-server"
            content = template.format(port=port, redis_cmd=redis_cmd)
        elif db_type == "mongodb" and not (user and password):
            # No-auth MongoDB: omit MONGO_INITDB_ROOT_* so container starts without auth
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
            _user = user or "dbuser"
            _pass = password or "dbpass"
            content = template.format(port=port, user=_user, password=_pass, database=database)

        compose_path = os.path.join(project_path, "docker-compose.yml")
        try:
            with open(compose_path, "w") as f:
                f.write(content)
            return compose_path
        except Exception:
            return ""

    def get_compose_db_info(self, compose_file: str, db_type: str) -> dict:
        return _extract_compose_db_info(compose_file, db_type)

    def get_running_ports(self, compose_file: str) -> dict:
        return _get_compose_running_ports(compose_file)

    def wait_for_port(self, host: str, port: int, timeout: int = 30) -> bool:
        for _ in range(timeout):
            try:
                with socket.create_connection((host, int(port)), timeout=1):
                    return True
            except OSError:
                time.sleep(1)
        return False

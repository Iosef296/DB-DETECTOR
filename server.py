import os
import sys
import json
import platform
import threading
import webbrowser
import subprocess
from pathlib import Path

_IS_WINDOWS = platform.system() == "Windows"

from flask import Flask, request, jsonify, send_from_directory, Response
from detector import DatabaseDetector
from connector import DBConnection

app = Flask(__name__, static_folder="static")

# Estado en memoria (sesión única)
_state = {
    "detection":       None,
    "connection":      None,   # DBConnection activa
    "credentials":     None,
    "compose_started": False,  # True after a successful docker compose up
    "app_proc":        None,   # Subprocess of the started application
    "app_log":         [],     # Circular log buffer (last 500 lines)
}


# ─── API ──────────────────────────────────────────────────────────────────────

@app.route("/api/detect", methods=["POST"])
def api_detect():
    data = request.json or {}
    path = data.get("path", "").strip()
    if not path:
        return jsonify({"error": "Proporciona una ruta de proyecto"}), 400
    path = os.path.expanduser(path)
    if not os.path.isdir(path):
        return jsonify({"error": f"Carpeta no encontrada: {path}"}), 404

    detector = DatabaseDetector(path)
    result = detector.detect()
    _state["detection"]       = result
    _state["compose_started"] = False
    # Pre-seleccionar credenciales de la BD principal
    primary = result.get("primary_db")
    if primary and primary in result.get("credentials", {}):
        _state["credentials"] = result["credentials"][primary]
    return jsonify(result)


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


def _generate_compose_file(project_path: str, db_type: str, creds: dict) -> str:
    """Genera un docker-compose.yml en el proyecto con las credenciales detectadas.

    Devuelve la ruta del archivo generado, o "" si no se puede generar.
    """
    if not project_path or not os.path.isdir(project_path):
        return ""
    template = _COMPOSE_TEMPLATES.get(db_type)
    if not template:
        return ""

    default_port = _DB_DEFAULT_PORTS.get(db_type, 5432)
    port     = creds.get("port") or default_port
    user     = creds.get("user") or "dbuser"
    password = creds.get("password") or "dbpass"
    database = creds.get("database") or db_type

    if db_type == "redis":
        redis_cmd = f'"redis-server --requirepass {password}"' if password else "redis-server"
        content = template.format(port=port, redis_cmd=redis_cmd)
    else:
        content = template.format(port=port, user=user, password=password, database=database)

    compose_path = os.path.join(project_path, "docker-compose.yml")
    try:
        with open(compose_path, "w") as f:
            f.write(content)
        return compose_path
    except Exception:
        return ""


@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    creds = data.get("credentials")
    if not creds:
        return jsonify({"error": "Sin credenciales"}), 400

    # Desconectar anterior si existe
    if _state["connection"]:
        try:
            _state["connection"].disconnect()
        except Exception:
            pass

    conn = DBConnection(creds)
    result = conn.connect()
    if result.get("ok"):
        _state["connection"] = conn
        _state["credentials"] = creds
        info = conn.get_server_info()
        return jsonify({"ok": True, "info": info})

    # Auth failure + compose not yet started → offer to start container
    if not result.get("install_hint") and not _state.get("compose_started"):
        err = (result.get("error") or "").lower()
        auth_fail = any(k in err for k in (
            "password authentication", "access denied", "authentication failed",
            "auth fail", "wrongpass", "noauth", "invalid password",
            "authentication error", "unauthorized",
        ))
        if auth_fail:
            db_type      = (creds.get("type") or "").lower()
            compose_file = (_state.get("detection") or {}).get("compose_file")

            # Si no hay compose file, generar uno con las credenciales del proyecto
            if not compose_file:
                project_path = (_state.get("detection") or {}).get("project_path", "")
                compose_file = _generate_compose_file(project_path, db_type, creds)
                if compose_file and _state.get("detection"):
                    _state["detection"]["compose_file"] = compose_file

            if compose_file:
                result["install_hint"] = {
                    "type": "server",
                    "db":   db_type,
                    "compose_conflict": True,
                }

    return jsonify(result), 400


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    if _state["connection"]:
        _state["connection"].disconnect()
        _state["connection"] = None
    return jsonify({"ok": True})


@app.route("/api/tables", methods=["GET"])
def api_tables():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    return jsonify(conn.list_tables())


@app.route("/api/query", methods=["POST"])
def api_query():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data       = request.json or {}
    query      = data.get("query", "").strip()
    include_id = data.get("include_id", False)
    if not query:
        return jsonify({"error": "Query vacía"}), 400
    return jsonify(conn.execute_query(query, include_id=include_id))


@app.route("/api/cloud/discover", methods=["GET"])
def api_cloud_discover():
    """Detecta proyectos cloud via CLIs instaladas."""
    import subprocess, shutil
    result = {"supabase": None, "fly": None}

    # ── Supabase CLI ──────────────────────────────────────────────────────────
    if shutil.which("supabase"):
        try:
            r = subprocess.run(
                ["supabase", "projects", "list", "--output", "json"],
                capture_output=True, text=True, timeout=15
            )
            if r.returncode == 0 and r.stdout.strip():
                projects = json.loads(r.stdout)
                result["supabase"] = {
                    "installed": True,
                    "logged_in": True,
                    "projects": [
                        {
                            "id":     p.get("id") or p.get("ref") or p.get("project_ref", ""),
                            "name":   p.get("name", ""),
                            "region": p.get("region", ""),
                            "host":   f"db.{p.get('id') or p.get('ref', '')}.supabase.co",
                        }
                        for p in (projects if isinstance(projects, list) else [])
                    ]
                }
            else:
                result["supabase"] = {"installed": True, "logged_in": False}
        except Exception as e:
            result["supabase"] = {"installed": True, "logged_in": False, "error": str(e)}
    else:
        result["supabase"] = {"installed": False}

    # ── Fly.io CLI ────────────────────────────────────────────────────────────
    fly_cmd = shutil.which("flyctl") or shutil.which("fly")
    if fly_cmd:
        try:
            r = subprocess.run(
                [fly_cmd, "postgres", "list", "--json"],
                capture_output=True, text=True, timeout=15
            )
            if r.returncode == 0 and r.stdout.strip():
                apps = json.loads(r.stdout)
                result["fly"] = {
                    "installed": True,
                    "logged_in": True,
                    "apps": [
                        {"name": a.get("Name", a.get("name", "")),
                         "status": a.get("Status", a.get("status", ""))}
                        for a in (apps if isinstance(apps, list) else [])
                    ]
                }
            else:
                # Intentar listar todas las apps si no hay postgres específico
                r2 = subprocess.run(
                    [fly_cmd, "apps", "list", "--json"],
                    capture_output=True, text=True, timeout=15
                )
                if r2.returncode == 0 and r2.stdout.strip():
                    apps = json.loads(r2.stdout)
                    result["fly"] = {
                        "installed": True,
                        "logged_in": True,
                        "apps": [
                            {"name": a.get("Name", a.get("name", "")),
                             "status": a.get("Status", a.get("status", ""))}
                            for a in (apps if isinstance(apps, list) else [])
                        ]
                    }
                else:
                    result["fly"] = {"installed": True, "logged_in": False}
        except Exception as e:
            result["fly"] = {"installed": True, "logged_in": False, "error": str(e)}
    else:
        result["fly"] = {"installed": False}

    return jsonify(result)


@app.route("/api/cloud/fly-proxy", methods=["POST"])
def api_fly_proxy():
    """Inicia un proxy local a una app de Postgres en Fly.io."""
    import subprocess, shutil, time
    fly_cmd = shutil.which("flyctl") or shutil.which("fly")
    if not fly_cmd:
        return jsonify({"ok": False, "error": "flyctl no instalado"}), 400

    app_name = (request.json or {}).get("app", "").strip()
    if not app_name:
        return jsonify({"ok": False, "error": "Nombre de app requerido"}), 400

    # Matar proxy anterior si existe
    if hasattr(api_fly_proxy, "_proc") and api_fly_proxy._proc:
        try:
            api_fly_proxy._proc.terminate()
        except Exception:
            pass

    try:
        proc = subprocess.Popen(
            [fly_cmd, "proxy", "15432:5432", "-a", app_name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        api_fly_proxy._proc = proc
        time.sleep(2)  # Esperar a que el proxy arranque
        if proc.poll() is not None:
            return jsonify({"ok": False, "error": "No se pudo iniciar el proxy"}), 400
        return jsonify({"ok": True, "local_port": 15432, "app": app_name})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/schema/<table>", methods=["GET"])
def api_schema(table):
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    return jsonify(conn.get_table_schema(table))


@app.route("/api/export", methods=["POST"])
def api_export():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data = request.json or {}
    table = data.get("table", "")
    fmt = data.get("format", "json")
    result = conn.export_table(table, fmt)
    if not result.get("ok"):
        return jsonify(result), 400
    mime = "application/json" if fmt == "json" else "text/csv"
    return Response(
        result["data"],
        mimetype=mime,
        headers={"Content-Disposition": f'attachment; filename="{result["filename"]}"'}
    )


def _find_free_port():
    import socket
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _port_in_use(port):
    import socket
    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False


def _patch_compose_ports(compose_file):
    """Detect occupied host ports in docker-compose.yml and remap them.

    Returns (new_content, port_remaps) where port_remaps is
    {old_host_port_str: new_host_port_str}.  If no conflicts, returns
    (original_content, {}).
    """
    import re
    with open(compose_file) as f:
        content = f.read()

    # Match port mappings in YAML: "HOST:CONTAINER" or HOST:CONTAINER (with optional quotes)
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

    new_content = pattern.sub(replace_port, content)
    return new_content, port_remaps


def _get_compose_running_ports(compose_file):
    """Devuelve {container_port: host_port} para servicios ya en ejecución de este compose."""
    try:
        r = subprocess.run(
            ["docker", "compose", "-f", compose_file, "ps", "--format", "json"],
            capture_output=True, text=True, timeout=10
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
                    target      = pub.get("TargetPort")
                    published   = pub.get("PublishedPort")
                    if target and published:
                        ports[target] = published
            except Exception:
                pass
        return ports
    except Exception:
        return {}


# Default container-side ports per DB type
_DB_CONTAINER_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mariadb":    3306,
    "mongodb":    27017,
    "redis":      6379,
}

# Env-var → credential field mappings per DB type
_DB_ENV_FIELDS = {
    "postgresql": {"user": ["POSTGRES_USER"], "password": ["POSTGRES_PASSWORD"], "database": ["POSTGRES_DB"]},
    "mysql":      {"user": ["MYSQL_USER", "MARIADB_USER"], "password": ["MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD"], "database": ["MYSQL_DATABASE", "MARIADB_DATABASE"]},
    "mariadb":    {"user": ["MARIADB_USER", "MYSQL_USER"], "password": ["MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD", "MYSQL_PASSWORD"], "database": ["MARIADB_DATABASE", "MYSQL_DATABASE"]},
    "mongodb":    {"user": ["MONGO_INITDB_ROOT_USERNAME"], "password": ["MONGO_INITDB_ROOT_PASSWORD"], "database": ["MONGO_INITDB_DATABASE"]},
    "redis":      {"password": ["REDIS_PASSWORD", "REQUIREPASS"]},
}


def _extract_compose_db_info(compose_file, db_type):
    """Parse docker-compose.yml to get the actual host port and credentials.

    Returns a dict with keys: host_port, user, password, database (any may be absent).
    """
    import re
    try:
        with open(compose_file) as f:
            content = f.read()
    except Exception:
        return {}

    container_port = _DB_CONTAINER_PORTS.get(db_type)
    info = {}

    # Find host port for this container port: matches "HOST:CONTAINER" patterns
    if container_port:
        # e.g. "39769:5432" or 39769:5432
        m = re.search(r'["\']?(\d+):' + str(container_port) + r'["\']?', content)
        if m:
            info["host_port"] = int(m.group(1))

    # Extract credentials from environment variables
    env_map = _DB_ENV_FIELDS.get(db_type, {})
    for field, env_vars in env_map.items():
        for var in env_vars:
            m = re.search(rf'{var}\s*[=:]\s*["\']?([^"\'\s\n]+)["\']?', content)
            if m:
                info[field] = m.group(1)
                break

    return info


@app.route("/api/compose/up", methods=["POST"])
def api_compose_up():
    import shutil, time, socket
    if not shutil.which("docker"):
        return jsonify({"ok": False, "error": "Docker no está instalado"}), 400

    compose_file = (_state.get("detection") or {}).get("compose_file")
    if not compose_file or not os.path.isfile(compose_file):
        return jsonify({"ok": False, "error": "No se encontró docker-compose.yml en el proyecto"}), 400

    project_dir = os.path.dirname(compose_file)

    # ── Si los servicios ya están corriendo, usar sus puertos reales ──────────
    running_ports = _get_compose_running_ports(compose_file)

    # ── Port conflict detection + patching (solo si NO están corriendo) ───────
    original_content = None
    port_remaps      = {}
    if not running_ports:
        try:
            patched_content, port_remaps = _patch_compose_ports(compose_file)
            if port_remaps:
                with open(compose_file) as f:
                    original_content = f.read()
                with open(compose_file, "w") as f:
                    f.write(patched_content)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Error al analizar docker-compose.yml: {e}"})

    try:
        result = subprocess.run(
            ["docker", "compose", "-f", compose_file, "up", "-d"],
            capture_output=True, text=True, timeout=120, cwd=project_dir
        )
        if result.returncode != 0:
            if original_content is not None:
                with open(compose_file, "w") as f:
                    f.write(original_content)
            return jsonify({"ok": False, "error": result.stderr.strip() or result.stdout.strip()})

        # ── Build credentials from compose file (host port + env vars) ────────
        base_creds   = dict(_state.get("credentials") or {})
        db_type      = (base_creds.get("type") or (_state.get("detection") or {}).get("primary_db") or "").lower()
        compose_info = _extract_compose_db_info(compose_file, db_type)

        # Si el contenedor ya estaba corriendo, sus puertos reales tienen prioridad
        if running_ports:
            container_port = _DB_CONTAINER_PORTS.get(db_type)
            if container_port and container_port in running_ports:
                compose_info["host_port"] = running_ports[container_port]

        creds = dict(base_creds)
        if compose_info.get("host_port"):
            creds["port"] = compose_info["host_port"]
        if compose_info.get("user"):
            creds["user"] = compose_info["user"]
        if compose_info.get("password"):
            creds["password"] = compose_info["password"]
        if compose_info.get("database"):
            creds["database"] = compose_info["database"]
        _state["credentials"] = creds

        # ── Wait until DB port responds ───────────────────────────────────────
        host = creds.get("host", "localhost")
        port = creds.get("port")
        if port and host in ("localhost", "127.0.0.1"):
            for _ in range(30):
                try:
                    with socket.create_connection((host, int(port)), timeout=1):
                        break
                except OSError:
                    time.sleep(1)
            else:
                return jsonify({"ok": False, "error": f"Contenedor arrancado pero el puerto {port} no responde. Espera unos segundos y reintenta."})
            # Extra pause — postgres/mysql need a moment after TCP is ready
            time.sleep(2)

        _state["compose_started"] = True
        response = {"ok": True, "output": result.stdout.strip(), "credentials": creds}
        if port_remaps:
            response["port_remaps"] = port_remaps
        return jsonify(response)
    except subprocess.TimeoutExpired:
        if original_content is not None:
            with open(compose_file, "w") as f:
                f.write(original_content)
        return jsonify({"ok": False, "error": "Tiempo de espera agotado levantando los servicios"})
    except Exception as e:
        if original_content is not None:
            with open(compose_file, "w") as f:
                f.write(original_content)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/redis/scan", methods=["GET"])
def api_redis_scan():
    conn = _state.get("connection")
    if not conn or conn.db_type != "redis":
        return jsonify({"error": "No hay conexión Redis activa"}), 400
    try:
        keys = conn._conn.keys("*")[:500]
        rows = []
        for k in keys:
            ktype = conn._conn.type(k)
            if ktype == "string":
                val = conn._conn.get(k) or ""
            elif ktype == "hash":
                val = str(conn._conn.hgetall(k))
            elif ktype == "list":
                val = str(conn._conn.lrange(k, 0, 4)) + ("…" if conn._conn.llen(k) > 5 else "")
            elif ktype == "set":
                members = list(conn._conn.smembers(k))[:5]
                val = str(members) + ("…" if conn._conn.scard(k) > 5 else "")
            elif ktype == "zset":
                val = str(conn._conn.zrange(k, 0, 4, withscores=True))
            else:
                val = f"<{ktype}>"
            rows.append([k, ktype, val])
        return jsonify({"ok": True, "columns": ["key", "type", "value"], "rows": rows, "rowcount": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/table/insert", methods=["POST"])
def api_insert():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data = request.json or {}
    return jsonify(conn.insert_row(data.get("table",""), data.get("values", {})))


@app.route("/api/table/update", methods=["POST"])
def api_update():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data = request.json or {}
    return jsonify(conn.update_row(
        data.get("table",""), data.get("pk_col",""), data.get("pk_val"), data.get("values", {})
    ))


@app.route("/api/table/delete", methods=["POST"])
def api_delete():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data = request.json or {}
    return jsonify(conn.delete_row(
        data.get("table",""), data.get("pk_col",""), data.get("pk_val")
    ))


@app.route("/api/install", methods=["POST"])
def api_install():
    data = request.json or {}
    package = data.get("package", "").strip()
    allowed = {"psycopg2-binary", "pymysql", "pymongo", "redis", "flask"}
    if not package or package not in allowed:
        return jsonify({"ok": False, "error": f"Paquete no permitido: {package}"}), 400
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", package],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            return jsonify({"ok": True, "output": result.stdout[-2000:]})
        return jsonify({"ok": False, "error": result.stderr[-1000:] or result.stdout[-1000:]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


_DOCKER_CONFIGS = {
    "postgresql": {
        "name": "dbdetector-postgres",
        "image": "postgres:16",
        "ports": ["5432:5432"],
        "env": {"POSTGRES_PASSWORD": "pass", "POSTGRES_USER": "postgres", "POSTGRES_DB": "postgres"},
        "default_creds": {"host": "localhost", "port": 5432, "user": "postgres", "password": "pass", "database": "postgres"},
    },
    "mysql": {
        "name": "dbdetector-mysql",
        "image": "mysql:8",
        "ports": ["3306:3306"],
        "env": {"MYSQL_ROOT_PASSWORD": "pass", "MYSQL_DATABASE": "mydb"},
        "default_creds": {"host": "localhost", "port": 3306, "user": "root", "password": "pass", "database": "mydb"},
    },
    "mongodb": {
        "name": "dbdetector-mongo",
        "image": "mongo:7",
        "ports": ["27017:27017"],
        "env": {},
        "default_creds": {"host": "localhost", "port": 27017, "user": "", "password": "", "database": ""},
    },
    "redis": {
        "name": "dbdetector-redis",
        "image": "redis:7",
        "ports": ["6379:6379"],
        "env": {},
        "default_creds": {"host": "localhost", "port": 6379, "user": "", "password": "", "database": "0"},
    },
}


@app.route("/api/install/server", methods=["POST"])
def api_install_server():
    import shutil, time
    data    = request.json or {}
    db_type = data.get("db_type", "").strip()
    cfg     = _DOCKER_CONFIGS.get(db_type)

    if not cfg:
        return jsonify({"ok": False, "error": f"Tipo de BD no soportado: {db_type}"}), 400
    if not shutil.which("docker"):
        return jsonify({"ok": False, "error": "Docker no está instalado. Instálalo desde https://docs.docker.com/get-docker/"}), 400

    name = cfg["name"]

    # Si ya existe el contenedor, solo arrancarlo
    check = subprocess.run(["docker", "inspect", name], capture_output=True)
    if check.returncode == 0:
        subprocess.run(["docker", "start", name], capture_output=True)
    else:
        cmd = ["docker", "run", "-d", "--name", name]
        for p in cfg["ports"]:
            cmd += ["-p", p]
        for k, v in cfg["env"].items():
            cmd += ["-e", f"{k}={v}"]
        cmd.append(cfg["image"])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return jsonify({"ok": False, "error": result.stderr.strip() or result.stdout.strip()})

    # Esperar hasta 20 s a que el puerto esté disponible
    import socket
    host_port = int(cfg["ports"][0].split(":")[0])
    for _ in range(20):
        try:
            with socket.create_connection(("localhost", host_port), timeout=1):
                break
        except OSError:
            time.sleep(1)
    else:
        return jsonify({"ok": False, "error": f"El contenedor arrancó pero el puerto {host_port} aún no responde. Espera unos segundos y reintenta."})

    return jsonify({"ok": True, "credentials": {**cfg["default_creds"], "type": db_type}})


_NATIVE_INSTALL = {
    "postgresql": {
        "linux": [
            "sudo apt-get update -y",
            "sudo apt-get install -y postgresql",
            "sudo systemctl start postgresql",
            "sudo systemctl enable postgresql",
        ],
        "windows_winget": [
            "winget install --id PostgreSQL.PostgreSQL --silent --accept-package-agreements --accept-source-agreements",
        ],
        "windows_choco": [
            "choco install postgresql --yes",
        ],
        "windows_msi": [
            "powershell -Command \"Invoke-WebRequest 'https://get.enterprisedb.com/postgresql/postgresql-16.2-1-windows-x64.exe' -OutFile $env:TEMP\\pg-setup.exe\"",
            "powershell -Command \"Start-Process '$env:TEMP\\pg-setup.exe' -Wait -ArgumentList '--mode unattended --superpassword pass'\"",
        ],
    },
    "mysql": {
        "linux": [
            "sudo apt-get update -y",
            "sudo apt-get install -y mysql-server",
            "sudo systemctl start mysql",
            "sudo systemctl enable mysql",
        ],
        "windows_winget": [
            "winget install --id Oracle.MySQL --silent --accept-package-agreements --accept-source-agreements",
            "net start MySQL80",
        ],
        "windows_choco": [
            "choco install mysql --yes",
        ],
        "windows_msi": [
            "powershell -Command \"Invoke-WebRequest 'https://dev.mysql.com/get/Downloads/MySQLInstaller/mysql-installer-community-8.0.36.0.msi' -OutFile $env:TEMP\\mysql-setup.msi\"",
            "powershell -Command \"Start-Process msiexec -Wait -ArgumentList '/i $env:TEMP\\mysql-setup.msi /quiet'\"",
        ],
    },
    "mongodb": {
        "linux": [
            "curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor",
            "echo \"deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/7.0 multiverse\" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list",
            "sudo apt-get update -y",
            "sudo apt-get install -y mongodb-org",
            "sudo systemctl start mongod",
            "sudo systemctl enable mongod",
        ],
        "windows_winget": [
            "winget install --id MongoDB.Server --silent --accept-package-agreements --accept-source-agreements",
            "net start MongoDB",
        ],
        "windows_choco": [
            "choco install mongodb --yes",
            "net start MongoDB",
        ],
        "windows_msi": [
            "powershell -Command \"$v='7.0.5'; Invoke-WebRequest \\\"https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-$v-signed.msi\\\" -OutFile $env:TEMP\\mongo-setup.msi\"",
            "powershell -Command \"Start-Process msiexec -Wait -ArgumentList '/i $env:TEMP\\mongo-setup.msi /quiet ADDLOCAL=all'\"",
            "net start MongoDB",
        ],
    },
    "redis": {
        "linux": [
            "sudo apt-get update -y",
            "sudo apt-get install -y redis-server",
            "sudo systemctl start redis-server",
            "sudo systemctl enable redis-server",
        ],
        "windows_winget": [
            "winget install --id Redis.Redis --silent --accept-package-agreements --accept-source-agreements",
            "net start Redis",
        ],
        "windows_choco": [
            "choco install redis-64 --yes",
            "redis-server --service-install",
            "net start Redis",
        ],
        "windows_wsl": [
            "wsl sudo apt-get install -y redis-server",
            "wsl sudo service redis-server start",
        ],
    },
}


@app.route("/api/install/server/native", methods=["POST"])
def api_install_server_native():
    import shutil, platform as _platform
    data     = request.json or {}
    db_type  = data.get("db_type", "").strip()
    platform = data.get("platform", "linux").strip()   # linux | windows
    method   = data.get("method", "auto").strip()      # auto | winget | choco | msi | wsl

    db_cfg = _NATIVE_INSTALL.get(db_type)
    if not db_cfg:
        return jsonify({"ok": False, "error": f"Tipo de BD no soportado: {db_type}"}), 400

    if platform == "windows":
        # Elegir método automáticamente si no se especificó
        if method == "auto":
            if shutil.which("winget"):
                method = "winget"
            elif shutil.which("choco"):
                method = "choco"
            else:
                method = "msi"
        key  = f"windows_{method}"
        cmds = db_cfg.get(key)
        if not cmds:
            return jsonify({"ok": False, "error": f"Método '{method}' no disponible para {db_type}"}), 400
        # En Windows ejecutar cada comando con PowerShell si no empieza por powershell/net/winget/choco
        run_cmd = lambda c: subprocess.run(
            ["powershell", "-NoProfile", "-Command", c] if not any(c.startswith(p) for p in ("powershell","net ","winget","choco","wsl","redis")) else c,
            shell=isinstance(c, str) and any(c.startswith(p) for p in ("net ","winget","choco","wsl","redis")),
            capture_output=True, text=True, timeout=180
        )
    else:
        cmds    = db_cfg.get(platform)
        run_cmd = lambda c: subprocess.run(c, shell=True, capture_output=True, text=True, timeout=180)

    if not cmds:
        return jsonify({"ok": False, "error": f"Instalación nativa no disponible para {db_type}/{platform}"}), 400

    output = []
    for cmd in cmds:
        try:
            result = run_cmd(cmd)
            output.append(f"$ {cmd}\n{(result.stdout or '').strip()}\n{(result.stderr or '').strip()}".strip())
            if result.returncode != 0:
                # net start puede devolver error si ya está corriendo — no es fatal
                if "net start" in cmd and result.returncode in (2, 3221225525):
                    continue
                return jsonify({"ok": False, "error": f"Falló: {cmd}\n{(result.stderr or result.stdout or '').strip()}"})
        except subprocess.TimeoutExpired:
            return jsonify({"ok": False, "error": f"Tiempo de espera agotado ejecutando: {cmd}"})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})

    return jsonify({"ok": True, "output": "\n\n".join(output)})


GRADLE_VERSION = "8.7"


def _gradle_bin() -> str:
    """Ruta al binario gradle instalado localmente (sin sudo)."""
    local = os.path.expanduser(f"~/.local/share/gradle-{GRADLE_VERSION}/bin/gradle")
    if os.path.isfile(local):
        return local
    import shutil
    return shutil.which("gradle") or ""


def _node_bin() -> str:
    if _IS_WINDOWS:
        local = os.path.join(os.environ.get("LOCALAPPDATA", ""), "node", "node.exe")
    else:
        local = os.path.expanduser("~/.local/share/node/bin/node")
    if os.path.isfile(local):
        return local
    import shutil
    return shutil.which("node") or ""


def _npm_bin() -> str:
    if _IS_WINDOWS:
        local = os.path.join(os.environ.get("LOCALAPPDATA", ""), "node", "npm.cmd")
    else:
        local = os.path.expanduser("~/.local/share/node/bin/npm")
    if os.path.isfile(local):
        return local
    import shutil
    return shutil.which("npm") or ""


def _runtime_install_script(missing: str) -> str:
    """Devuelve un script bash que instala el runtime sin sudo."""
    if missing == "gradle":
        gradle_dir  = os.path.expanduser("~/.local/share")
        gradle_home = os.path.join(gradle_dir, f"gradle-{GRADLE_VERSION}")
        java_home   = os.path.expanduser("~/.local/share/java-21")
        return f"""
set -e

# ── Java (sin sudo) ──────────────────────────────────────────────────────────
JAVA_HOME_LOCAL="{java_home}"
if command -v java &>/dev/null; then
  export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
  echo "==> Java ya instalado: $(java -version 2>&1 | head -1)"
elif [ -f "$JAVA_HOME_LOCAL/bin/java" ]; then
  export JAVA_HOME="$JAVA_HOME_LOCAL"
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "==> Java local encontrado: $(java -version 2>&1 | head -1)"
else
  echo "==> Descargando OpenJDK 21 (sin sudo)…"
  mkdir -p "$JAVA_HOME_LOCAL"
  curl -fsSL "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse" -o /tmp/java-dl.tar.gz
  echo "==> Extrayendo Java…"
  tar -xzf /tmp/java-dl.tar.gz -C "$JAVA_HOME_LOCAL" --strip-components=1
  rm -f /tmp/java-dl.tar.gz
  export JAVA_HOME="$JAVA_HOME_LOCAL"
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "==> Java listo: $(java -version 2>&1 | head -1)"
fi

# ── Gradle (sin sudo) ────────────────────────────────────────────────────────
GRADLE_HOME="{gradle_home}"
if [ ! -f "$GRADLE_HOME/bin/gradle" ]; then
  ZIP=/tmp/gradle-{GRADLE_VERSION}-bin.zip
  echo "==> Descargando Gradle {GRADLE_VERSION}…"
  curl -fsSL "https://services.gradle.org/distributions/gradle-{GRADLE_VERSION}-bin.zip" -o "$ZIP"
  echo "==> Extrayendo Gradle…"
  mkdir -p "{gradle_dir}"
  unzip -q -o "$ZIP" -d "{gradle_dir}"
  rm -f "$ZIP"
  chmod +x "$GRADLE_HOME/bin/gradle"
else
  echo "==> Gradle ya instalado en $GRADLE_HOME"
fi
echo "==> Gradle listo: $(JAVA_HOME=$JAVA_HOME "$GRADLE_HOME/bin/gradle" --version | grep '^Gradle')"
""".strip()
    if missing == "mvn":
        return "sudo apt-get install -y maven && mvn --version"
    if missing == "node":
        if _IS_WINDOWS:
            node_dir = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), "node")
            return f"""
$ErrorActionPreference = 'Stop'
$NodeDir = '{node_dir}'
if (Test-Path (Join-Path $NodeDir 'node.exe')) {{
    $v = & "$NodeDir\\node.exe" --version
    Write-Host "==> Node.js ya instalado: $v"
}} else {{
    Write-Host '==> Obteniendo version LTS de Node.js...'
    try {{
        $releases = Invoke-RestMethod 'https://nodejs.org/dist/index.json' -UseBasicParsing
        $lts = $releases | Where-Object {{ $_.lts }} | Select-Object -First 1
        $NodeVer = $lts.version
    }} catch {{
        $NodeVer = 'v20.19.1'
    }}
    $arch = if ([Environment]::Is64BitOperatingSystem) {{ 'x64' }} else {{ 'x86' }}
    Write-Host "==> Descargando Node.js $NodeVer ($arch) sin admin..."
    $zipUrl = "https://nodejs.org/dist/$NodeVer/node-$NodeVer-win-$arch.zip"
    $zipPath = Join-Path $env:TEMP 'node-dl.zip'
    Invoke-WebRequest $zipUrl -OutFile $zipPath -UseBasicParsing
    Write-Host '==> Extrayendo Node.js...'
    New-Item -ItemType Directory -Force -Path $NodeDir | Out-Null
    $extractDir = Join-Path $env:TEMP 'node-extract'
    if (Test-Path $extractDir) {{ Remove-Item $extractDir -Recurse -Force }}
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force
    $innerDir = Get-ChildItem $extractDir | Select-Object -First 1
    Copy-Item (Join-Path $innerDir.FullName '*') $NodeDir -Recurse -Force
    Remove-Item $zipPath, $extractDir -Recurse -Force
}}
$nv = & "$NodeDir\\node.exe" --version
$npmv = & "$NodeDir\\npm.cmd" --version
Write-Host "==> Node.js listo: $nv"
Write-Host "==> npm listo: $npmv"
""".strip()
        else:
            node_dir = os.path.expanduser("~/.local/share/node")
            return f"""
set -e
NODE_DIR="{node_dir}"
if [ -f "$NODE_DIR/bin/node" ]; then
  echo "==> Node.js ya instalado: $($NODE_DIR/bin/node --version)"
else
  ARCH=$(uname -m)
  case "$ARCH" in
    x86_64)  ARCH_SUFFIX="x64" ;;
    aarch64) ARCH_SUFFIX="arm64" ;;
    armv7l)  ARCH_SUFFIX="armv7l" ;;
    *)       echo "Arquitectura no soportada: $ARCH"; exit 1 ;;
  esac
  echo "==> Obteniendo versión LTS de Node.js…"
  NODE_VER=$(curl -fsSL "https://nodejs.org/dist/index.json" | python3 -c "import sys,json; r=[x for x in json.load(sys.stdin) if x['lts']]; print(r[0]['version'])" 2>/dev/null || echo "v20.19.1")
  echo "==> Descargando Node.js $NODE_VER ($ARCH_SUFFIX) sin sudo…"
  mkdir -p "$NODE_DIR"
  curl -fsSL "https://nodejs.org/dist/$NODE_VER/node-$NODE_VER-linux-$ARCH_SUFFIX.tar.gz" -o /tmp/node-dl.tar.gz
  echo "==> Extrayendo Node.js…"
  tar -xzf /tmp/node-dl.tar.gz -C "$NODE_DIR" --strip-components=1
  rm -f /tmp/node-dl.tar.gz
fi
echo "==> Node.js listo: $($NODE_DIR/bin/node --version)"
echo "==> npm listo: $($NODE_DIR/bin/npm --version)"
""".strip()
    return ""


@app.route("/api/app/install-runtime", methods=["POST"])
def api_app_install_runtime():
    app_info = (_state.get("detection") or {}).get("app_start")
    if not app_info or not app_info.get("missing"):
        return jsonify({"ok": False, "error": "No hay runtime pendiente de instalar"}), 400

    missing = app_info["missing"]
    script  = _runtime_install_script(missing)
    if not script:
        return jsonify({"ok": False, "error": f"Instalación automática no disponible para: {missing}"}), 400

    # Matar proceso anterior si existe
    prev = _state.get("app_proc")
    if prev and prev.poll() is None:
        try: prev.terminate()
        except Exception: pass

    _state["app_log"] = []

    try:
        if _IS_WINDOWS:
            shell_cmd = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script]
        else:
            shell_cmd = ["bash", "-c", script]
        proc = subprocess.Popen(
            shell_cmd,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        _state["app_proc"] = proc

        cwd         = app_info.get("cwd", ".")
        db_type     = app_info.get("type", "")
        label       = app_info.get("label", missing)
        missing_    = missing
        run_script_ = app_info.get("run_script", "start")

        def _reader():
            for line in proc.stdout:
                _state["app_log"].append(line.rstrip())
                if len(_state["app_log"]) > 500:
                    _state["app_log"].pop(0)
            proc.wait()
            if proc.returncode == 0:
                if missing_ == "gradle":
                    new_cmd = f'"{_gradle_bin()}" bootRun'
                elif missing_ == "node":
                    npm_path = _npm_bin()
                    new_cmd = f'"{npm_path}" run {run_script_}' if npm_path else None
                else:
                    new_cmd = None
                if new_cmd and _state.get("detection"):
                    _state["detection"]["app_start"] = {
                        "type": db_type, "label": label,
                        "cmd": new_cmd, "cwd": cwd,
                    }
                _state["app_log"].append(f"__INSTALL_OK__ {new_cmd or ''}")
            else:
                _state["app_log"].append(f"__INSTALL_FAIL__ código {proc.returncode}")

        threading.Thread(target=_reader, daemon=True).start()
        return jsonify({"ok": True, "streaming": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/app/start", methods=["POST"])
def api_app_start():
    app_info = (_state.get("detection") or {}).get("app_start")
    if not app_info:
        return jsonify({"ok": False, "error": "No se detectó cómo iniciar la aplicación"}), 400
    if not app_info.get("cmd"):
        fix = app_info.get("fix", "Instala el runtime necesario para este proyecto")
        return jsonify({"ok": False, "error": fix}), 400

    # Kill previous process if still running
    prev = _state.get("app_proc")
    if prev and prev.poll() is None:
        try:
            prev.terminate()
        except Exception:
            pass

    _state["app_log"] = []
    cmd = app_info["cmd"]
    cwd = app_info.get("cwd", ".")

    # ── Detectar conflicto de puerto: container remapeado vs app hardcodeada ───
    _std_ports = {"postgresql": 5432, "mysql": 3306, "mongodb": 27017, "redis": 6379}
    _early_creds = _state.get("credentials") or {}
    _db_type_pre  = (_early_creds.get("type") or "").lower()
    _std_port     = _std_ports.get(_db_type_pre)
    _actual_port  = _early_creds.get("port")
    if _std_port and _actual_port and int(_actual_port) != int(_std_port) and _port_in_use(_std_port):
        # El container está en un puerto alternativo pero la app intentará el estándar
        _state["app_log"].append(
            f"⚠️  AVISO: el contenedor Docker está en el puerto {_actual_port} pero hay "
            f"un servicio local ocupando el puerto estándar {_std_port}. "
            f"La aplicación intentará conectarse al servicio local y fallará."
        )
        _state["app_log"].append(f"==> Intentando detener el servicio local de {_db_type_pre}…")
        _stop_cmds = {
            "postgresql": ["pg_ctlcluster stop", "service postgresql stop",
                           "systemctl stop postgresql"],
            "mysql":      ["service mysql stop", "systemctl stop mysql",
                           "service mysqld stop"],
            "mongodb":    ["service mongod stop", "systemctl stop mongod"],
            "redis":      ["service redis-server stop", "systemctl stop redis"],
        }
        _freed = False
        for _scmd in _stop_cmds.get(_db_type_pre, []):
            try:
                # Solo sudo -n (no interactivo): nunca pide contraseña, falla silenciosamente
                _r = subprocess.run(
                    f"sudo -n {_scmd}", shell=True, capture_output=True, text=True, timeout=10
                )
                if _r.returncode == 0 and not _port_in_use(_std_port):
                    _state["app_log"].append(f"==> Servicio local detenido con 'sudo {_scmd}'.")
                    _freed = True
                    break
            except Exception:
                pass
        if not _freed and _port_in_use(_std_port):
            _state["app_log"].append(
                f"⚠️  No se pudo detener el servicio local en el puerto {_std_port}. "
                f"Arrancando la app de todos modos — las credenciales del puerto {_actual_port} ya están inyectadas."
            )
        # Si se liberó el puerto, mover el container al puerto estándar
        if _freed:
            compose_file = (_state.get("detection") or {}).get("compose_file")
            if compose_file:
                try:
                    with open(compose_file) as _f:
                        _compose_content = _f.read()
                    import re as _re
                    _compose_content = _re.sub(
                        rf'["\']?{_actual_port}:{_std_port}["\']?',
                        f'{_std_port}:{_std_port}',
                        _compose_content
                    )
                    with open(compose_file, "w") as _f:
                        _f.write(_compose_content)
                    subprocess.run(
                        ["docker", "compose", "-f", compose_file, "up", "-d"],
                        capture_output=True, text=True, timeout=60,
                        cwd=os.path.dirname(compose_file)
                    )
                    _early_creds["port"] = _std_port
                    _state["credentials"] = _early_creds
                    _state["app_log"].append(
                        f"==> Contenedor reiniciado en el puerto estándar {_std_port}."
                    )
                except Exception as _e:
                    _state["app_log"].append(f"==> Aviso al reiniciar container: {_e}")

    # Node.js: si no existe node_modules, anteponer npm install al comando
    if app_info.get("type") == "nodejs":
        node_modules = os.path.join(cwd, "node_modules")
        if not os.path.isdir(node_modules):
            if _IS_WINDOWS:
                _local_npm = os.path.join(os.environ.get("LOCALAPPDATA", ""), "node", "npm.cmd")
            else:
                _local_npm = os.path.expanduser("~/.local/share/node/bin/npm")
            _npm_exe = f'"{_local_npm}"' if os.path.isfile(_local_npm) else "npm"
            cmd = f"{_npm_exe} install && {cmd}"

    env = os.environ.copy()

    # JAVA_HOME: sistema → local descargado
    if "JAVA_HOME" not in env:
        try:
            java_path = subprocess.check_output(
                "readlink -f $(which java) 2>/dev/null || true",
                shell=True, text=True
            ).strip()
            if java_path:
                env["JAVA_HOME"] = os.path.dirname(os.path.dirname(java_path))
        except Exception:
            pass
    java_local = os.path.expanduser("~/.local/share/java-21")
    if "JAVA_HOME" not in env and os.path.isfile(os.path.join(java_local, "bin", "java")):
        env["JAVA_HOME"] = java_local

    # PATH: gradle local + java local
    _path_sep = ";" if _IS_WINDOWS else ":"
    extra_paths = []
    gradle_bin_dir = os.path.expanduser(f"~/.local/share/gradle-{GRADLE_VERSION}/bin")
    if os.path.isdir(gradle_bin_dir):
        extra_paths.append(gradle_bin_dir)
    if env.get("JAVA_HOME"):
        extra_paths.append(os.path.join(env["JAVA_HOME"], "bin"))
    if _IS_WINDOWS:
        local_node_bin = os.path.join(os.environ.get("LOCALAPPDATA", ""), "node")
    else:
        local_node_bin = os.path.expanduser("~/.local/share/node/bin")
    if os.path.isdir(local_node_bin):
        extra_paths.append(local_node_bin)
    if extra_paths:
        env["PATH"] = _path_sep.join(extra_paths) + _path_sep + env.get("PATH", "")

    # Inyectar credenciales de BD para que la app use el puerto/usuario correcto
    creds   = _state.get("credentials") or {}
    db_type = (creds.get("type") or "").lower()
    host    = creds.get("host", "localhost")
    port    = creds.get("port", "")
    db      = creds.get("database", "")
    user    = creds.get("user", "")
    pwd     = creds.get("password", "")

    app_type = app_info.get("type", "")

    # ── Spring Boot ───────────────────────────────────────────────────────────
    if app_type == "springboot":
        if db_type == "postgresql" and port:
            env["SPRING_DATASOURCE_URL"]      = f"jdbc:postgresql://{host}:{port}/{db}"
            env["SPRING_DATASOURCE_USERNAME"] = user
            env["SPRING_DATASOURCE_PASSWORD"] = pwd
        elif db_type in ("mysql", "mariadb") and port:
            driver = "mariadb" if db_type == "mariadb" else "mysql"
            env["SPRING_DATASOURCE_URL"]      = f"jdbc:{driver}://{host}:{port}/{db}"
            env["SPRING_DATASOURCE_USERNAME"] = user
            env["SPRING_DATASOURCE_PASSWORD"] = pwd
        elif db_type == "mongodb" and port:
            uri = f"mongodb://{user}:{pwd}@{host}:{port}/{db}" if user else f"mongodb://{host}:{port}/{db}"
            env["SPRING_DATA_MONGODB_URI"] = uri
        elif db_type == "redis" and port:
            env["SPRING_DATA_REDIS_HOST"]     = host
            env["SPRING_DATA_REDIS_PORT"]     = str(port)
            if pwd:
                env["SPRING_DATA_REDIS_PASSWORD"] = pwd

    # ── Node.js ───────────────────────────────────────────────────────────────
    elif app_type == "nodejs":
        if db_type == "postgresql" and port:
            env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
        elif db_type in ("mysql", "mariadb") and port:
            env["DATABASE_URL"] = f"mysql://{user}:{pwd}@{host}:{port}/{db}"
        elif db_type == "mongodb" and port:
            env["MONGODB_URI"] = f"mongodb://{user}:{pwd}@{host}:{port}/{db}" if user else f"mongodb://{host}:{port}/{db}"
            env["DATABASE_URL"] = env["MONGODB_URI"]
        elif db_type == "redis" and port:
            env["REDIS_URL"] = f"redis://:{pwd}@{host}:{port}" if pwd else f"redis://{host}:{port}"

    # ── Django / Python ───────────────────────────────────────────────────────
    elif app_type in ("django", "python"):
        if db_type == "postgresql" and port:
            env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
        elif db_type in ("mysql", "mariadb") and port:
            env["DATABASE_URL"] = f"mysql://{user}:{pwd}@{host}:{port}/{db}"
        elif db_type == "mongodb" and port:
            env["MONGODB_URI"]  = f"mongodb://{host}:{port}/{db}"
        elif db_type == "redis" and port:
            env["REDIS_URL"]    = f"redis://:{pwd}@{host}:{port}" if pwd else f"redis://{host}:{port}"
        if db_type in ("postgresql", "mysql", "mariadb") and port:
            env["DB_HOST"] = host; env["DB_PORT"] = str(port)
            env["DB_NAME"] = db;   env["DB_USER"] = user; env["DB_PASSWORD"] = pwd

    # ── Rails ─────────────────────────────────────────────────────────────────
    elif app_type == "rails":
        if db_type == "postgresql" and port:
            env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
        elif db_type in ("mysql", "mariadb") and port:
            env["DATABASE_URL"] = f"mysql2://{user}:{pwd}@{host}:{port}/{db}"
        elif db_type == "mongodb" and port:
            env["DATABASE_URL"] = f"mongodb://{host}:{port}/{db}"

    # ── Laravel / PHP ─────────────────────────────────────────────────────────
    elif app_type == "laravel":
        if db_type == "postgresql":
            env["DB_CONNECTION"] = "pgsql"; env["DB_HOST"] = host
            env["DB_PORT"] = str(port);     env["DB_DATABASE"] = db
            env["DB_USERNAME"] = user;      env["DB_PASSWORD"] = pwd
        elif db_type in ("mysql", "mariadb"):
            env["DB_CONNECTION"] = "mysql"; env["DB_HOST"] = host
            env["DB_PORT"] = str(port);     env["DB_DATABASE"] = db
            env["DB_USERNAME"] = user;      env["DB_PASSWORD"] = pwd
        elif db_type == "mongodb":
            env["DB_CONNECTION"] = "mongodb"; env["DB_HOST"] = host
            env["DB_PORT"] = str(port);       env["DB_DATABASE"] = db
        elif db_type == "redis":
            env["REDIS_HOST"] = host; env["REDIS_PORT"] = str(port)
            if pwd: env["REDIS_PASSWORD"] = pwd

    # ── Go ────────────────────────────────────────────────────────────────────
    elif app_type == "go":
        if db_type == "postgresql" and port:
            env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
        elif db_type in ("mysql", "mariadb") and port:
            env["DATABASE_URL"] = f"{user}:{pwd}@tcp({host}:{port})/{db}"
        elif db_type == "mongodb" and port:
            env["MONGODB_URI"]  = f"mongodb://{host}:{port}/{db}"
        elif db_type == "redis" and port:
            env["REDIS_ADDR"]   = f"{host}:{port}"

    try:
        proc = subprocess.Popen(
            cmd, shell=True, cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        _state["app_proc"] = proc

        def _reader():
            for line in proc.stdout:
                _state["app_log"].append(line.rstrip())
                if len(_state["app_log"]) > 500:
                    _state["app_log"].pop(0)
            proc.wait()

        threading.Thread(target=_reader, daemon=True).start()
        return jsonify({"ok": True, "pid": proc.pid, "cmd": cmd})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/app/logs", methods=["GET"])
def api_app_logs():
    proc   = _state.get("app_proc")
    lines  = list(_state.get("app_log", []))
    status = "stopped"
    if proc:
        rc = proc.poll()
        if rc is None:
            status = "running"
        elif rc == 0:
            status = "exited_ok"
        else:
            status = f"exited_{rc}"
    return jsonify({"ok": True, "lines": lines, "status": status})


@app.route("/api/app/stop", methods=["POST"])
def api_app_stop():
    proc = _state.get("app_proc")
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    _state["app_proc"] = None
    return jsonify({"ok": True})


@app.route("/api/status", methods=["GET"])
def api_status():
    return jsonify({
        "connected": _state["connection"] is not None,
        "credentials": _state["credentials"],
        "detection": _state["detection"],
    })


# ─── Frontend ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    port = int(os.environ.get("PORT", 7432))
    url = f"http://localhost:{port}"
    print(f"\n{'─'*50}")
    print(f"  🔍 DB Detector corriendo en {url}")
    print(f"{'─'*50}\n")

    # Abrir browser automáticamente (excepto en modo silencioso)
    if "--no-browser" not in sys.argv:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()

    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()

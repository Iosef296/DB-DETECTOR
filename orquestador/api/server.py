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

# Add parent to path for core imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.orchestrator import Orchestrator
from core.detector import DatabaseDetector
from core.connector import DBConnection

# ── Storage path relative to this file's grandparent ─────────────────────────
_ROOT = Path(__file__).parent.parent
_DB_PATH = str(_ROOT / "storage" / "projects.db")

orchestrator = Orchestrator(_DB_PATH)
app = Flask(__name__, static_folder=str(_ROOT / "static"))

# ── Legacy _state (for backwards-compatible single-project endpoints) ─────────
_state = {
    "detection":       None,
    "connection":      None,
    "credentials":     None,
    "compose_started": False,
    "app_proc":        None,
    "app_log":         [],
}


# ══════════════════════════════════════════════════════════════════════════════
# MULTI-PROJECT ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/projects", methods=["POST"])
def api_projects_add():
    data = request.json or {}
    path = data.get("path", "").strip()
    if not path:
        return jsonify({"error": "Proporciona una ruta de proyecto"}), 400
    result = orchestrator.add_project(path)
    if not result.get("ok"):
        return jsonify({"error": result.get("error")}), 400
    return jsonify(result), 201


@app.route("/api/projects", methods=["GET"])
def api_projects_list():
    return jsonify(orchestrator.list_projects())


@app.route("/api/projects/<name>", methods=["GET"])
def api_projects_status(name):
    row = orchestrator.status(name)
    if not row:
        return jsonify({"error": f"Proyecto '{name}' no encontrado"}), 404
    return jsonify(row)


@app.route("/api/projects/<name>", methods=["DELETE"])
def api_projects_remove(name):
    result = orchestrator.remove_project(name)
    if not result.get("ok"):
        return jsonify({"error": result.get("error")}), 400
    return jsonify({"ok": True})


@app.route("/api/projects/<name>/up", methods=["POST"])
def api_projects_up(name):
    result = orchestrator.up(name)
    if not result.get("ok"):
        return jsonify({"error": result.get("error")}), 400
    return jsonify(result)


@app.route("/api/projects/<name>/down", methods=["POST"])
def api_projects_down(name):
    result = orchestrator.down(name)
    if not result.get("ok"):
        return jsonify({"error": result.get("error")}), 400
    return jsonify(result)


@app.route("/api/projects/<name>/restart", methods=["POST"])
def api_projects_restart(name):
    result = orchestrator.restart(name)
    if not result.get("ok"):
        return jsonify({"error": result.get("error")}), 400
    return jsonify(result)


@app.route("/api/projects/<name>/logs", methods=["GET"])
def api_projects_logs(name):
    lines = int(request.args.get("lines", 200))
    return jsonify({"logs": orchestrator.get_logs(name, lines)})


@app.route("/api/projects/<name>/query", methods=["POST"])
def api_projects_query(name):
    conn = orchestrator.get_connection(name)
    if not conn:
        return jsonify({"error": f"No se pudo conectar al proyecto '{name}'"}), 400
    data  = request.json or {}
    query = data.get("sql") or data.get("collection") or data.get("command", "")
    if not query:
        return jsonify({"error": "Query vacía"}), 400
    return jsonify(conn.execute_query(query))


@app.route("/api/projects/<name>/tables", methods=["GET"])
def api_projects_tables(name):
    conn = orchestrator.get_connection(name)
    if not conn:
        return jsonify({"error": f"No se pudo conectar al proyecto '{name}'"}), 400
    return jsonify(conn.list_tables())


@app.route("/api/projects/<name>/export", methods=["POST"])
def api_projects_export(name):
    conn = orchestrator.get_connection(name)
    if not conn:
        return jsonify({"error": f"No se pudo conectar al proyecto '{name}'"}), 400
    data   = request.json or {}
    table  = data.get("table", "")
    fmt    = data.get("format", "json")
    result = conn.export_table(table, fmt)
    if not result.get("ok"):
        return jsonify(result), 400
    mime = "application/json" if fmt == "json" else "text/csv"
    return Response(
        result["data"],
        mimetype=mime,
        headers={"Content-Disposition": f'attachment; filename="{result["filename"]}"'}
    )


@app.route("/api/projects/up-all", methods=["POST"])
def api_projects_up_all():
    return jsonify(orchestrator.up_all())


@app.route("/api/projects/down-all", methods=["POST"])
def api_projects_down_all():
    return jsonify(orchestrator.down_all())


# ══════════════════════════════════════════════════════════════════════════════
# LEGACY ENDPOINTS (single-project compatibility)
# ══════════════════════════════════════════════════════════════════════════════

# Import helpers from parent server.py equivalents
from core.docker_manager import (
    _patch_compose_ports, _find_free_port, _port_in_use,
    _get_compose_running_ports, _extract_compose_db_info,
    _DB_CONTAINER_PORTS, _DB_DEFAULT_PORTS,
)

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


def _generate_compose_file(project_path, db_type, creds):
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
        with open(compose_path, "w", encoding="utf-8") as f:
            f.write(content)
        return compose_path
    except Exception:
        return ""


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
    primary = result.get("primary_db")
    if primary and primary in result.get("credentials", {}):
        _state["credentials"] = result["credentials"][primary]
    return jsonify(result)


@app.route("/api/connect", methods=["POST"])
def api_connect():
    data  = request.json or {}
    creds = data.get("credentials")
    if not creds:
        return jsonify({"error": "Sin credenciales"}), 400
    if _state["connection"]:
        try:
            _state["connection"].disconnect()
        except Exception:
            pass
    conn   = DBConnection(creds)
    result = conn.connect()
    if result.get("ok"):
        _state["connection"] = conn
        _state["credentials"] = creds
        info = conn.get_server_info()
        return jsonify({"ok": True, "info": info})
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
            if not compose_file:
                project_path = (_state.get("detection") or {}).get("project_path", "")
                compose_file = _generate_compose_file(project_path, db_type, creds)
                if compose_file and _state.get("detection"):
                    _state["detection"]["compose_file"] = compose_file
            if compose_file:
                result["install_hint"] = {"type": "server", "db": db_type, "compose_conflict": True}
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
    data   = request.json or {}
    table  = data.get("table", "")
    fmt    = data.get("format", "json")
    result = conn.export_table(table, fmt)
    if not result.get("ok"):
        return jsonify(result), 400
    mime = "application/json" if fmt == "json" else "text/csv"
    return Response(
        result["data"],
        mimetype=mime,
        headers={"Content-Disposition": f'attachment; filename="{result["filename"]}"'}
    )


@app.route("/api/compose/up", methods=["POST"])
def api_compose_up():
    import shutil, time, socket as _socket
    if not shutil.which("docker"):
        return jsonify({"ok": False, "error": "Docker no está instalado"}), 400
    compose_file = (_state.get("detection") or {}).get("compose_file")
    if not compose_file or not os.path.isfile(compose_file):
        return jsonify({"ok": False, "error": "No se encontró docker-compose.yml en el proyecto"}), 400
    project_dir = os.path.dirname(compose_file)
    running_ports = _get_compose_running_ports(compose_file)
    original_content = None
    port_remaps = {}
    if not running_ports:
        try:
            patched_content, port_remaps = _patch_compose_ports(compose_file)
            if port_remaps:
                with open(compose_file, encoding="utf-8", errors="replace") as f:
                    original_content = f.read()
                with open(compose_file, "w", encoding="utf-8") as f:
                    f.write(patched_content)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Error al analizar docker-compose.yml: {e}"})
    try:
        result = subprocess.run(
            ["docker", "compose", "-f", compose_file, "up", "-d"],
            capture_output=True, encoding="utf-8", errors="replace", timeout=120, cwd=project_dir
        )
        if result.returncode != 0:
            if original_content is not None:
                with open(compose_file, "w", encoding="utf-8") as f:
                    f.write(original_content)
            return jsonify({"ok": False, "error": result.stderr.strip() or result.stdout.strip()})
        base_creds   = dict(_state.get("credentials") or {})
        db_type      = (base_creds.get("type") or (_state.get("detection") or {}).get("primary_db") or "").lower()
        compose_info = _extract_compose_db_info(compose_file, db_type)
        if running_ports:
            container_port = _DB_CONTAINER_PORTS.get(db_type)
            if container_port and container_port in running_ports:
                compose_info["host_port"] = running_ports[container_port]
        creds = dict(base_creds)
        if compose_info.get("host_port"):
            creds["port"] = compose_info["host_port"]
        for field in ("user", "password", "database"):
            if compose_info.get(field):
                creds[field] = compose_info[field]
        _state["credentials"] = creds
        host = creds.get("host", "localhost")
        port = creds.get("port")
        if port and host in ("localhost", "127.0.0.1"):
            for _ in range(30):
                try:
                    with _socket.create_connection((host, int(port)), timeout=1):
                        break
                except OSError:
                    time.sleep(1)
            else:
                return jsonify({"ok": False, "error": f"Contenedor arrancado pero el puerto {port} no responde."})
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


@app.route("/api/table/insert", methods=["POST"])
def api_insert():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data = request.json or {}
    return jsonify(conn.insert_row(data.get("table", ""), data.get("values", {})))


@app.route("/api/table/update", methods=["POST"])
def api_update():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data = request.json or {}
    return jsonify(conn.update_row(
        data.get("table", ""), data.get("pk_col", ""), data.get("pk_val"), data.get("values", {})
    ))


@app.route("/api/table/delete", methods=["POST"])
def api_delete():
    conn = _state.get("connection")
    if not conn:
        return jsonify({"error": "No hay conexión activa"}), 400
    data = request.json or {}
    return jsonify(conn.delete_row(
        data.get("table", ""), data.get("pk_col", ""), data.get("pk_val")
    ))


@app.route("/api/install", methods=["POST"])
def api_install():
    data    = request.json or {}
    package = data.get("package", "").strip()
    allowed = {"psycopg2-binary", "pymysql", "pymongo", "redis", "flask"}
    if not package or package not in allowed:
        return jsonify({"ok": False, "error": f"Paquete no permitido: {package}"}), 400
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", package],
            capture_output=True, encoding="utf-8", errors="replace", timeout=120
        )
        if result.returncode == 0:
            return jsonify({"ok": True, "output": result.stdout[-2000:]})
        return jsonify({"ok": False, "error": result.stderr[-1000:] or result.stdout[-1000:]})
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
    prev = _state.get("app_proc")
    if prev and prev.poll() is None:
        try:
            prev.terminate()
        except Exception:
            pass
    _state["app_log"] = []
    cmd = app_info["cmd"]
    cwd = app_info.get("cwd", ".")
    env = os.environ.copy()
    try:
        proc = subprocess.Popen(
            cmd, shell=True, cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            encoding="utf-8", errors="replace", bufsize=1,
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


@app.route("/api/status", methods=["GET"])
def api_status_legacy():
    return jsonify({
        "connected":   _state["connection"] is not None,
        "credentials": _state["credentials"],
        "detection":   _state["detection"],
    })


# ── Install server endpoint (legacy) ─────────────────────────────────────────

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
    import shutil, time, socket as _socket
    data    = request.json or {}
    db_type = data.get("db_type", "").strip()
    cfg     = _DOCKER_CONFIGS.get(db_type)
    if not cfg:
        return jsonify({"ok": False, "error": f"Tipo de BD no soportado: {db_type}"}), 400
    if not shutil.which("docker"):
        return jsonify({"ok": False, "error": "Docker no está instalado"}), 400
    name  = cfg["name"]
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
        result = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace", timeout=120)
        if result.returncode != 0:
            return jsonify({"ok": False, "error": result.stderr.strip() or result.stdout.strip()})
    host_port = int(cfg["ports"][0].split(":")[0])
    for _ in range(20):
        try:
            with _socket.create_connection(("localhost", host_port), timeout=1):
                break
        except OSError:
            time.sleep(1)
    else:
        return jsonify({"ok": False, "error": f"El contenedor arrancó pero el puerto {host_port} aún no responde."})
    return jsonify({"ok": True, "credentials": {**cfg["default_creds"], "type": db_type}})


# ── Frontend routes ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(_ROOT / "static"), "index.html")


@app.route("/legacy")
def legacy():
    legacy_static = str(_ROOT.parent / "static")
    return send_from_directory(legacy_static, "index.html")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    port = int(os.environ.get("PORT", 7433))
    url  = f"http://localhost:{port}"
    print(f"\n{'─'*50}")
    print(f"  ORQUESTADOR corriendo en {url}")
    print(f"  Interfaz clasica: {url}/legacy")
    print(f"{'─'*50}\n")
    if "--no-browser" not in sys.argv:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()

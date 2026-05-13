import os
import json
import sqlite3
import threading
import subprocess
from datetime import datetime
from pathlib import Path

from core.detector import DatabaseDetector
from core.connector import DBConnection
from core.port_manager import PortManager
from core.docker_manager import DockerManager


_DB_CONTAINER_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mariadb":    3306,
    "mongodb":    27017,
    "redis":      6379,
}


class Orchestrator:
    def __init__(self, db_path: str):
        self.db_path      = db_path
        self.port_manager = PortManager(db_path)
        self.docker       = DockerManager()
        # In-memory state per project
        self._connections: dict[str, DBConnection]  = {}
        self._procs:       dict[str, subprocess.Popen] = {}
        self._logs:        dict[str, list]           = {}
        self._lock = threading.Lock()
        self._init_db()

    # ── SQLite setup ──────────────────────────────────────────────────────────

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        con = sqlite3.connect(self.db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT UNIQUE NOT NULL,
                path        TEXT NOT NULL,
                db_type     TEXT,
                framework   TEXT,
                app_port    INTEGER,
                db_port     INTEGER,
                status      TEXT DEFAULT 'STOPPED',
                credentials TEXT,
                detection   TEXT,
                created_at  TEXT,
                updated_at  TEXT
            )
        """)
        con.commit()
        con.close()

    def _con(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    def _row_to_dict(self, row) -> dict:
        if row is None:
            return {}
        d = dict(row)
        for field in ("credentials", "detection"):
            if d.get(field):
                try:
                    d[field] = json.loads(d[field])
                except Exception:
                    pass
        return d

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add_project(self, path: str) -> dict:
        path = os.path.abspath(os.path.expanduser(path))
        if not os.path.isdir(path):
            return {"ok": False, "error": f"Carpeta no encontrada: {path}"}

        name = Path(path).name

        con = self._con()
        existing = con.execute("SELECT id FROM projects WHERE name = ?", (name,)).fetchone()
        con.close()
        if existing:
            return {"ok": False, "error": f"El proyecto '{name}' ya existe"}

        detector = DatabaseDetector(path)
        detection = detector.detect()

        db_type   = detection.get("primary_db")
        app_start = detection.get("app_start") or {}
        framework = app_start.get("type")
        creds     = detection.get("credentials", {}).get(db_type) if db_type else {}

        db_port, app_port = self.port_manager.assign_ports(name)

        now = self._now()
        con = self._con()
        con.execute("""
            INSERT INTO projects
              (name, path, db_type, framework, app_port, db_port, status,
               credentials, detection, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'STOPPED', ?, ?, ?, ?)
        """, (
            name, path, db_type, framework, app_port, db_port,
            json.dumps(creds) if creds else None,
            json.dumps(detection),
            now, now,
        ))
        con.commit()
        con.close()

        return {
            "ok":        True,
            "name":      name,
            "path":      path,
            "db_type":   db_type,
            "framework": framework,
            "app_port":  app_port,
            "db_port":   db_port,
            "status":    "STOPPED",
        }

    def remove_project(self, name: str) -> dict:
        row = self._get_row(name)
        if not row:
            return {"ok": False, "error": f"Proyecto '{name}' no encontrado"}

        if row.get("status") in ("RUNNING", "STARTING"):
            self.down(name)

        self.port_manager.release_ports(name)
        self.docker.remove_network(name)

        con = self._con()
        con.execute("DELETE FROM projects WHERE name = ?", (name,))
        con.commit()
        con.close()

        with self._lock:
            self._connections.pop(name, None)
            self._procs.pop(name, None)
            self._logs.pop(name, None)

        return {"ok": True}

    def _get_row(self, name: str) -> dict:
        con = self._con()
        row = con.execute("SELECT * FROM projects WHERE name = ?", (name,)).fetchone()
        con.close()
        return self._row_to_dict(row)

    def _set_status(self, name: str, status: str):
        con = self._con()
        con.execute(
            "UPDATE projects SET status = ?, updated_at = ? WHERE name = ?",
            (status, self._now(), name)
        )
        con.commit()
        con.close()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def up(self, name: str) -> dict:
        row = self._get_row(name)
        if not row:
            return {"ok": False, "error": f"Proyecto '{name}' no encontrado"}

        # Kill any existing app process before starting fresh
        with self._lock:
            old_proc = self._procs.pop(name, None)
        if old_proc and old_proc.poll() is None:
            try:
                old_proc.terminate()
                old_proc.wait(timeout=5)
            except Exception:
                try:
                    old_proc.kill()
                except Exception:
                    pass

        self._set_status(name, "STARTING")
        path    = row["path"]
        db_type = row.get("db_type")
        creds   = row.get("credentials") or {}
        db_port = row.get("db_port")

        self.docker.create_network(name)

        detection = row.get("detection") or {}
        compose_file = detection.get("compose_file")

        if not compose_file or not os.path.isfile(compose_file):
            if db_type:
                compose_file = self.docker.generate_compose(
                    path, db_type, creds, {"db_port": db_port}
                )

        if compose_file and os.path.isfile(compose_file):
            result = self.docker.up(path, f"orq_{name}_net", {"db_port": db_port})
            if not result.get("ok"):
                self._set_status(name, "ERROR")
                return result

            # Extract real credentials from compose after possible port remapping
            compose_info = self.docker.get_compose_db_info(compose_file, db_type or "")
            running_ports = self.docker.get_running_ports(compose_file)
            if running_ports:
                container_port = _DB_CONTAINER_PORTS.get(db_type or "")
                if container_port and container_port in running_ports:
                    compose_info["host_port"] = running_ports[container_port]

            if compose_info.get("host_port"):
                creds = dict(creds)
                creds["port"] = compose_info["host_port"]
                if compose_info.get("user"):
                    creds["user"] = compose_info["user"]
                if compose_info.get("password"):
                    creds["password"] = compose_info["password"]
                if compose_info.get("database"):
                    creds["database"] = compose_info["database"]
                con = self._con()
                con.execute(
                    "UPDATE projects SET credentials = ?, updated_at = ? WHERE name = ?",
                    (json.dumps(creds), self._now(), name)
                )
                con.commit()
                con.close()

            # Wait for DB port
            host = creds.get("host", "localhost")
            port = creds.get("port")
            if port and host in ("localhost", "127.0.0.1"):
                ready = self.docker.wait_for_port(host, int(port), timeout=30)
                if not ready:
                    self._set_status(name, "ERROR")
                    return {"ok": False, "error": f"Contenedor arrancó pero puerto {port} no responde"}

        # Start application process
        app_start = detection.get("app_start") or {}
        app_port  = row.get("app_port")
        app_proc  = self._start_app(name, app_start, creds, db_type, app_port)
        if app_proc:
            with self._lock:
                self._procs[name] = app_proc

        self._set_status(name, "RUNNING")
        return {"ok": True, "app_url": f"http://localhost:{app_port}"}

    def _start_app(self, name: str, app_start: dict,
                   creds: dict, db_type: str,
                   app_port: int = None) -> "subprocess.Popen | None":
        if not app_start or not app_start.get("cmd"):
            return None

        cmd = app_start["cmd"]
        cwd = app_start.get("cwd", ".")
        env = os.environ.copy()
        if app_port:
            env["PORT"] = str(app_port)

        host = creds.get("host", "localhost")
        port = creds.get("port", "")
        db   = creds.get("database", "")
        user = creds.get("user", "")
        pwd  = creds.get("password", "")
        app_type = app_start.get("type", "")

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
                if user and pwd:
                    uri = f"mongodb://{user}:{pwd}@{host}:{port}/{db}?authSource=admin"
                else:
                    uri = f"mongodb://{host}:{port}/{db}"
                env["SPRING_DATA_MONGODB_URI"] = uri
            elif db_type == "redis" and port:
                env["SPRING_DATA_REDIS_HOST"] = host
                env["SPRING_DATA_REDIS_PORT"] = str(port)
                if pwd:
                    env["SPRING_DATA_REDIS_PASSWORD"] = pwd

        elif app_type == "nodejs":
            if db_type == "postgresql" and port:
                env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type in ("mysql", "mariadb") and port:
                env["DATABASE_URL"] = f"mysql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type == "mongodb" and port:
                if user and pwd:
                    _mongo_uri = f"mongodb://{user}:{pwd}@{host}:{port}/{db}?authSource=admin"
                else:
                    _mongo_uri = f"mongodb://{host}:{port}/{db}"
                for _k in ("MONGODB_URI", "MONGO_URI", "MONGO_URL", "DATABASE_URL",
                           "DB_URI", "MONGO_CONNECTION_STRING", "MONGODB_URL"):
                    env[_k] = _mongo_uri
            elif db_type == "redis" and port:
                env["REDIS_URL"] = f"redis://:{pwd}@{host}:{port}" if pwd else f"redis://{host}:{port}"

        elif app_type in ("django", "python"):
            if db_type == "postgresql" and port:
                env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type in ("mysql", "mariadb") and port:
                env["DATABASE_URL"] = f"mysql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type == "mongodb" and port:
                env["MONGODB_URI"] = f"mongodb://{host}:{port}/{db}"
            elif db_type == "redis" and port:
                env["REDIS_URL"] = f"redis://:{pwd}@{host}:{port}" if pwd else f"redis://{host}:{port}"
            if db_type in ("postgresql", "mysql", "mariadb") and port:
                env["DB_HOST"] = host
                env["DB_PORT"] = str(port)
                env["DB_NAME"] = db
                env["DB_USER"] = user
                env["DB_PASSWORD"] = pwd

        elif app_type == "rails":
            if db_type == "postgresql" and port:
                env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type in ("mysql", "mariadb") and port:
                env["DATABASE_URL"] = f"mysql2://{user}:{pwd}@{host}:{port}/{db}"

        elif app_type == "laravel":
            if db_type == "postgresql":
                env.update({"DB_CONNECTION": "pgsql", "DB_HOST": host, "DB_PORT": str(port),
                            "DB_DATABASE": db, "DB_USERNAME": user, "DB_PASSWORD": pwd})
            elif db_type in ("mysql", "mariadb"):
                env.update({"DB_CONNECTION": "mysql", "DB_HOST": host, "DB_PORT": str(port),
                            "DB_DATABASE": db, "DB_USERNAME": user, "DB_PASSWORD": pwd})

        elif app_type == "go":
            if db_type == "postgresql" and port:
                env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type in ("mysql", "mariadb") and port:
                env["DATABASE_URL"] = f"{user}:{pwd}@tcp({host}:{port})/{db}"
            elif db_type == "mongodb" and port:
                env["MONGODB_URI"] = f"mongodb://{host}:{port}/{db}"
            elif db_type == "redis" and port:
                env["REDIS_ADDR"] = f"{host}:{port}"

        try:
            proc = subprocess.Popen(
                cmd, shell=True, cwd=cwd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding="utf-8", errors="replace", bufsize=1,
            )

            def _reader():
                for line in proc.stdout:
                    self.append_log(name, line.rstrip())
                proc.wait()

            threading.Thread(target=_reader, daemon=True).start()
            return proc
        except Exception as e:
            self.append_log(name, f"Error iniciando aplicación: {e}")
            return None

    def down(self, name: str) -> dict:
        row = self._get_row(name)
        if not row:
            return {"ok": False, "error": f"Proyecto '{name}' no encontrado"}

        # Stop app process
        with self._lock:
            proc = self._procs.pop(name, None)
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        # Stop Docker
        self.docker.down(row["path"])

        # Disconnect DB
        with self._lock:
            conn = self._connections.pop(name, None)
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass

        self._set_status(name, "STOPPED")
        return {"ok": True}

    def restart(self, name: str) -> dict:
        self.down(name)
        return self.up(name)

    def status(self, name: str) -> dict:
        row = self._get_row(name)
        if not row:
            return {}
        docker_status = self.docker.status(row.get("path", ""))
        row["docker"] = docker_status
        return row

    def list_projects(self) -> list:
        con = self._con()
        rows = con.execute("SELECT * FROM projects ORDER BY created_at").fetchall()
        con.close()
        return [self._row_to_dict(r) for r in rows]

    def up_all(self) -> dict:
        projects = self.list_projects()
        started, errors = [], []
        for p in projects:
            if p.get("status") == "STOPPED":
                result = self.up(p["name"])
                if result.get("ok"):
                    started.append(p["name"])
                else:
                    errors.append({"name": p["name"], "error": result.get("error")})
        return {"started": started, "errors": errors}

    def down_all(self) -> dict:
        projects = self.list_projects()
        stopped, errors = [], []
        for p in projects:
            if p.get("status") in ("RUNNING", "STARTING"):
                result = self.down(p["name"])
                if result.get("ok"):
                    stopped.append(p["name"])
                else:
                    errors.append({"name": p["name"], "error": result.get("error")})
        return {"stopped": stopped, "errors": errors}

    def get_connection(self, name: str) -> "DBConnection | None":
        with self._lock:
            existing = self._connections.get(name)
        if existing:
            return existing

        row = self._get_row(name)
        if not row:
            return None
        creds = row.get("credentials")
        if not creds:
            return None

        # Build raw_url for MongoDB so connector uses auth + authSource=admin
        creds = dict(creds)
        if (creds.get("type") or row.get("db_type") or "").lower() == "mongodb":
            host = creds.get("host", "localhost")
            port = creds.get("port", 27017)
            user = creds.get("user", "")
            pwd  = creds.get("password", "")
            db   = creds.get("database", "")
            if user and pwd:
                creds["raw_url"] = f"mongodb://{user}:{pwd}@{host}:{port}/{db}?authSource=admin"
            else:
                creds["raw_url"] = f"mongodb://{host}:{port}/{db}"

        conn = DBConnection(creds)
        result = conn.connect()
        if result.get("ok"):
            with self._lock:
                self._connections[name] = conn
            return conn
        return None

    def append_log(self, name: str, line: str):
        with self._lock:
            buf = self._logs.setdefault(name, [])
            buf.append(line)
            if len(buf) > 500:
                buf.pop(0)

    def get_logs(self, name: str, lines: int = 200) -> list:
        with self._lock:
            buf = self._logs.get(name, [])
            return list(buf[-lines:])

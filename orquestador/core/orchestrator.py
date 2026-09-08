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
from core.manager_factory import get_manager

# Puerto estándar de cada motor de BD dentro del contenedor Docker
_DB_CONTAINER_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mariadb":    3306,
    "mongodb":    27017,
    "redis":      6379,
}


# ── Clase principal ────────────────────────────────────────────────────────────
# Gestor es el cerebro del orquestador: registra proyectos en SQLite,
# detecta su BD, asigna puertos, arranca/para Docker y el proceso de la app.

class Gestor:
    def __init__(self, db_path: str):
        # Ruta al archivo SQLite que persiste el estado de todos los proyectos
        self.db_path      = db_path
        # PortManager garantiza que cada proyecto tenga puertos únicos
        self.port_manager = PortManager(db_path)
        # get_manager() elige Docker si está disponible, sino PortableManager
        self.docker       = get_manager()
        # Conexiones DB activas en memoria (clave = nombre del proyecto)
        self._connections: dict[str, DBConnection]  = {}
        # Procesos de la app corriendo (subprocess.Popen por proyecto)
        self._procs:       dict[str, subprocess.Popen] = {}
        # Buffer de logs en memoria, máximo 500 líneas por proyecto
        self._logs:        dict[str, list]           = {}
        # Lock para que varios hilos no corrompan los dicts anteriores
        self._lock = threading.Lock()
        # Inicializar esquema SQLite si la tabla no existe todavía
        self._init_db()

    # ── SQLite setup ──────────────────────────────────────────────────────────

    def _init_db(self):
        # Crear el directorio del archivo si no existe (ej. orquestador/storage/)
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
        # row_factory permite acceder a columnas por nombre (row["name"] en vez de row[0])
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    def _row_to_dict(self, row) -> dict:
        # Convierte una Row de SQLite a dict y deserializa los campos JSON
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
        # Resolver ~ y rutas relativas a ruta absoluta
        path = os.path.abspath(os.path.expanduser(path))
        if not os.path.isdir(path):
            return {"ok": False, "error": f"Carpeta no encontrada: {path}"}

        # El nombre del proyecto es el nombre de la carpeta
        name = Path(path).name

        con = self._con()
        existing = con.execute("SELECT id FROM projects WHERE name = ?", (name,)).fetchone()
        con.close()
        if existing:
            return {"ok": False, "error": f"El proyecto '{name}' ya existe"}

        # Correr el detector para encontrar qué BD usa el proyecto
        detector = DatabaseDetector(path)
        detection = detector.detect()

        db_type   = detection.get("primary_db")
        app_start = detection.get("app_start") or {}
        framework = app_start.get("type")
        # Extraer credenciales específicas de la BD primaria detectada
        creds     = detection.get("credentials", {}).get(db_type) if db_type else {}

        # Proyecto solo-frontend (React, Vue, etc.): no necesita BD ni Docker
        if self._has_fe(path) and not self._has_be(path):
            db_type = None
            creds   = {}
            framework = framework or "frontend"

        # Reservar puertos únicos para esta app y su BD
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

    # ── Scan helpers ──────────────────────────────────────────────────────────
    # Estas listas determinan si un proyecto tiene backend (necesita BD/Docker)
    # o solo frontend (no necesita Docker).

    _BE_INDICATORS = [
        # Archivos que solo existen en proyectos backend
        "pom.xml", "build.gradle", "build.gradle.kts", "gradlew", "mvnw",
        "manage.py", "requirements.txt", "Gemfile", "artisan", "go.mod", "main.go",
    ]
    _FE_INDICATORS = ["package.json"]
    _FE_FRAMEWORKS = {"react", "vue", "vite", "angular", "next", "nuxt", "svelte",
                      "solid", "preact", "astro", "remix", "gatsby"}
    # Carpetas con nombres típicos de subdivisión BE/FE dentro de un mono-repo
    _SPLIT_NAMES   = {"backend", "frontend", "api", "client", "server", "web",
                      "app", "ui", "spa", "service", "services"}

    # Dependencias npm que indican que el proyecto es un servidor Node, no solo FE
    _BE_SERVER_DEPS = {
        "express", "fastify", "koa", "hapi", "nestjs", "@nestjs/core",
        "pg", "mysql2", "mysql", "mongoose", "sequelize", "typeorm",
        "prisma", "@prisma/client", "knex", "redis", "mongodb",
    }
    _BE_SERVER_DIRS = {"server", "backend", "api", "services", "service"}

    def _has_be(self, path: str) -> bool:
        # Comprobar archivos típicos de BE en la raíz
        if any(os.path.exists(os.path.join(path, f)) for f in self._BE_INDICATORS):
            return True
        # docker-compose en la raíz indica que hay servicios de BD
        for cf in ("docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"):
            if os.path.exists(os.path.join(path, cf)):
                return True
        # Subcarpeta server/ o backend/ con su propio package.json → Node backend
        for d in self._BE_SERVER_DIRS:
            sub_pkg = os.path.join(path, d, "package.json")
            if os.path.exists(sub_pkg):
                return True
        # package.json en la raíz con dependencias de servidor Node
        pkg = os.path.join(path, "package.json")
        if os.path.exists(pkg):
            try:
                import json as _json
                data = _json.loads(open(pkg, encoding="utf-8", errors="replace").read())
                deps = {**data.get("dependencies", {}), **data.get("devDependencies", {})}
                if self._BE_SERVER_DEPS & set(deps.keys()):
                    return True
            except Exception:
                pass
        return False

    def _has_fe(self, path: str) -> bool:
        # Un proyecto tiene FE si package.json lista frameworks de frontend
        pkg = os.path.join(path, "package.json")
        if not os.path.exists(pkg):
            return False
        try:
            import json as _json
            data = _json.loads(open(pkg, encoding="utf-8", errors="replace").read())
            deps = {**data.get("dependencies", {}), **data.get("devDependencies", {})}
            return bool(self._FE_FRAMEWORKS & set(deps.keys()))
        except Exception:
            return True  # tiene package.json → asumir frontend

    def _add_one(self, path: str, added: list, skipped: list, errors: list):
        name = Path(path).name
        result = self.add_project(path)
        if result.get("ok"):
            added.append(result)
        elif "ya existe" in result.get("error", ""):
            skipped.append(name)
        else:
            errors.append({"name": name, "error": result.get("error")})

    def scan_folder(self, folder: str) -> dict:
        """Escanea una carpeta y registra todos los sub-proyectos automáticamente."""
        folder = os.path.abspath(os.path.expanduser(folder))
        if not os.path.isdir(folder):
            return {"ok": False, "error": f"Carpeta no encontrada: {folder}"}

        added, skipped, errors = [], [], []
        try:
            entries = sorted(os.scandir(folder), key=lambda e: e.name)
        except PermissionError as e:
            return {"ok": False, "error": str(e)}

        for entry in entries:
            if not entry.is_dir():
                continue
            path = entry.path

            # Buscar si hay subcarpetas con nombres BE/FE (mono-repo estilo)
            try:
                sub_entries = [e for e in os.scandir(path) if e.is_dir()]
            except PermissionError:
                sub_entries = []

            # Subcarpetas con nombre típico Y que tengan código detectable
            split_subs = [e for e in sub_entries
                          if e.name.lower() in self._SPLIT_NAMES
                          and (self._has_be(e.path) or self._has_fe(e.path))]

            if split_subs:
                # Mono-repo con BE/FE separados → agregar cada uno por separado
                for sub in split_subs:
                    self._add_one(sub.path, added, skipped, errors)
                continue

            # Proyecto normal con BE o FE en la raíz
            is_be = self._has_be(path)
            is_fe = self._has_fe(path)

            if is_be or is_fe:
                self._add_one(path, added, skipped, errors)
            else:
                skipped.append(entry.name)

        return {"ok": True, "added": added, "skipped": skipped, "errors": errors}

    def remove_project(self, name: str) -> dict:
        row = self._get_row(name)
        if not row:
            return {"ok": False, "error": f"Proyecto '{name}' no encontrado"}

        # Apagar el proyecto antes de borrarlo si está corriendo
        if row.get("status") in ("RUNNING", "STARTING"):
            self.down(name)

        # Liberar los puertos para que otros proyectos los puedan reutilizar
        self.port_manager.release_ports(name)
        self.docker.remove_network(name)

        con = self._con()
        con.execute("DELETE FROM projects WHERE name = ?", (name,))
        con.commit()
        con.close()

        # Limpiar estado en memoria
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

        # Matar proceso anterior si quedó colgado de un up() previo
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

        # Proyectos frontend-only no necesitan Docker ni BD
        is_frontend_only = not db_type

        if not is_frontend_only:
            self.docker.create_network(name)

        detection = row.get("detection") or {}
        compose_file = detection.get("compose_file")

        # Si no hay compose.yml existente, generarlo desde la plantilla para la BD detectada
        if not is_frontend_only:
            if not compose_file or not os.path.isfile(compose_file):
                if db_type:
                    compose_file = self.docker.generate_compose(
                        path, db_type, creds, {"db_port": db_port}
                    )

        # Levantar Docker Compose (o PortableManager si no hay Docker)
        if not is_frontend_only and compose_file and os.path.isfile(compose_file):
            result = self.docker.up(path, f"orq_{name}_net", {"db_port": db_port})
            if not result.get("ok"):
                self._set_status(name, "ERROR")
                return result

            # Leer credenciales reales del compose (puede haber remapeo de puertos por conflicto)
            compose_info = self.docker.get_compose_db_info(compose_file, db_type or "")
            running_ports = self.docker.get_running_ports(compose_file)
            if running_ports:
                container_port = _DB_CONTAINER_PORTS.get(db_type or "")
                if container_port and container_port in running_ports:
                    compose_info["host_port"] = running_ports[container_port]

            if compose_info.get("host_port"):
                # Actualizar credenciales con el puerto real que Docker asignó
                real_db_port = compose_info["host_port"]
                creds = dict(creds)
                creds["port"] = real_db_port
                if compose_info.get("user"):
                    creds["user"] = compose_info["user"]
                if compose_info.get("password"):
                    creds["password"] = compose_info["password"]
                if compose_info.get("database"):
                    creds["database"] = compose_info["database"]
                con = self._con()
                con.execute(
                    "UPDATE projects SET credentials = ?, db_port = ?, updated_at = ? WHERE name = ?",
                    (json.dumps(creds), real_db_port, self._now(), name)
                )
                con.commit()
                con.close()
            elif db_port:
                # Modo nativo/portable: no hay host_port en compose_info.
                # Usar el db_port asignado originalmente para mantener consistencia.
                creds = dict(creds)
                creds["port"] = db_port
                con = self._con()
                con.execute(
                    "UPDATE projects SET credentials = ?, db_port = ?, updated_at = ? WHERE name = ?",
                    (json.dumps(creds), db_port, self._now(), name)
                )
                con.commit()
                con.close()

            # Esperar a que el puerto de la BD responda antes de arrancar la app
            host = creds.get("host", "localhost")
            port = creds.get("port")
            if port and host in ("localhost", "127.0.0.1"):
                ready = self.docker.wait_for_port("localhost", int(port), timeout=30)
                if not ready:
                    self._set_status(name, "ERROR")
                    return {"ok": False, "error": f"Contenedor arrancó pero puerto {port} no responde"}

        # Arrancar el proceso de la aplicación (npm run dev, python manage.py, etc.)
        app_start = detection.get("app_start") or {}
        app_port  = row.get("app_port")
        app_proc  = self._start_app(name, app_start, creds, db_type, app_port)
        if app_proc:
            with self._lock:
                self._procs[name] = app_proc

        self._set_status(name, "RUNNING")
        updated = self._get_row(name)
        return {
            "ok":      True,
            "app_url": f"http://localhost:{app_port}",
            "app_port": app_port,
            "db_port":  updated.get("db_port"),
        }

    def _start_app(self, name: str, app_start: dict,
                   creds: dict, db_type: str,
                   app_port: int = None) -> "subprocess.Popen | None":
        # Si el detector no encontró cómo arrancar la app, no hacemos nada
        if not app_start or not app_start.get("cmd"):
            return None

        cmd = app_start["cmd"]
        cwd = app_start.get("cwd", ".")
        # Heredar el entorno del sistema y añadir variables de BD
        env = os.environ.copy()
        if app_port:
            env["PORT"] = str(app_port)
            env["SERVER_PORT"] = str(app_port)  # Spring Boot usa SERVER_PORT

        host = creds.get("host", "localhost")
        if host == "127.0.0.1":
            host = "localhost"
        port = creds.get("port", "")
        db   = creds.get("database", "")
        user = creds.get("user", "")
        pwd  = creds.get("password", "")
        app_type = app_start.get("type", "")

        # Inyectar variables de entorno según el framework detectado.
        # Cada framework tiene sus propios nombres de variables de BD.

        if app_type == "springboot":
            # Spring Boot lee la BD de SPRING_DATASOURCE_URL
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
            # Node.js usa DATABASE_URL en formato URL de conexión
            if db_type == "postgresql" and port:
                env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type in ("mysql", "mariadb") and port:
                env["DATABASE_URL"] = f"mysql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type == "mongodb" and port:
                if user and pwd:
                    _mongo_uri = f"mongodb://{user}:{pwd}@{host}:{port}/{db}?authSource=admin"
                else:
                    _mongo_uri = f"mongodb://{host}:{port}/{db}"
                # Distintas librerías usan distintas variables → las poblamos todas
                for _k in ("MONGODB_URI", "MONGO_URI", "MONGO_URL", "DATABASE_URL",
                           "DB_URI", "MONGO_CONNECTION_STRING", "MONGODB_URL"):
                    env[_k] = _mongo_uri
            elif db_type == "redis" and port:
                env["REDIS_URL"] = f"redis://:{pwd}@{host}:{port}" if pwd else f"redis://{host}:{port}"

        elif app_type in ("django", "python"):
            # Django/Flask pueden usar DATABASE_URL o variables individuales
            if db_type == "postgresql" and port:
                env["DATABASE_URL"] = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type in ("mysql", "mariadb") and port:
                env["DATABASE_URL"] = f"mysql://{user}:{pwd}@{host}:{port}/{db}"
            elif db_type == "mongodb" and port:
                env["MONGODB_URI"] = f"mongodb://{host}:{port}/{db}"
            elif db_type == "redis" and port:
                env["REDIS_URL"] = f"redis://:{pwd}@{host}:{port}" if pwd else f"redis://{host}:{port}"
            # Variables individuales DB_HOST / DB_PORT / etc. para compatibilidad
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
            # Laravel usa variables DB_* en lugar de DATABASE_URL
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
                # Go usa el formato DSN de MySQL: user:pass@tcp(host:port)/db
                env["DATABASE_URL"] = f"{user}:{pwd}@tcp({host}:{port})/{db}"
            elif db_type == "mongodb" and port:
                env["MONGODB_URI"] = f"mongodb://{host}:{port}/{db}"
            elif db_type == "redis" and port:
                env["REDIS_ADDR"] = f"{host}:{port}"

        try:
            # Lanzar el proceso con stdout capturado para poder leer logs
            proc = subprocess.Popen(
                cmd, shell=True, cwd=cwd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding="utf-8", errors="replace", bufsize=1,
            )

            # Hilo daemon que lee stdout/stderr línea a línea y los guarda en _logs
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

        # Parar proceso de la app (SIGTERM → esperar 5s → SIGKILL si sigue vivo)
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

        # Bajar contenedores Docker (o detener proceso nativo)
        self.docker.down(row["path"])

        # Cerrar conexión activa a la BD si existe
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
        # Parar limpio y luego volver a arrancar
        self.down(name)
        return self.up(name)

    def status(self, name: str) -> dict:
        row = self._get_row(name)
        if not row:
            return {}
        # Enriquecer con estado real de los contenedores Docker
        docker_status = self.docker.status(row.get("path", ""))
        row["docker"] = docker_status
        return row

    def list_projects(self) -> list:
        con = self._con()
        rows = con.execute("SELECT * FROM projects ORDER BY created_at").fetchall()
        con.close()
        return [self._row_to_dict(r) for r in rows]

    def up_all(self) -> dict:
        # Arrancar solo los proyectos en estado STOPPED (ignorar RUNNING/ERROR)
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
        # Parar solo los proyectos en estado RUNNING o STARTING
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
        # Devolver conexión cacheada si ya existe (evitar reconectar en cada query)
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

        # MongoDB necesita raw_url con authSource=admin para autenticarse correctamente
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
            # Solo cachear si la conexión fue exitosa
            with self._lock:
                self._connections[name] = conn
            return conn
        return None

    def append_log(self, name: str, line: str):
        # Buffer circular: se descarta la línea más antigua cuando llega a 500
        with self._lock:
            buf = self._logs.setdefault(name, [])
            buf.append(line)
            if len(buf) > 500:
                buf.pop(0)

    def get_logs(self, name: str, lines: int = 200) -> list:
        # Devolver las últimas N líneas del buffer
        with self._lock:
            buf = self._logs.get(name, [])
            return list(buf[-lines:])

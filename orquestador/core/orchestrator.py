import os
import re
import time
import json
import stat
import shutil
import socket
import sqlite3
import zipfile
import tempfile
import platform
import threading
import subprocess
from datetime import datetime
from pathlib import Path

_IS_WINDOWS = platform.system() == "Windows"

# Sub-servicios de un bundle de infra (obs, kafka) que tienen UI web propia
# navegable. "promtail" y "kafka" (broker binario) se excluyen a propósito:
# no sirven nada por HTTP que un usuario quiera abrir en el navegador.
_BROWSABLE_INFRA_SERVICES = {"grafana", "prometheus", "loki", "kafbat"}

from core.detector import DatabaseDetector, detect_infra_service
from core.connector import DBConnection
from core.port_manager import PortManager
from core.manager_factory import get_manager
from core.docker_manager import DockerManager
from core.infra_manager import PortableInfraManager
from core.paths import projects_dir, data_root


def _rmtree(path: str):
    """rmtree que además quita el bit de solo-lectura (Windows: los objetos de
    .git son read-only y hacen fallar shutil.rmtree normal)."""
    def _onerror(func, p, _exc):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except Exception:
            pass
    shutil.rmtree(path, onerror=_onerror)

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
        # Arranque nativo de infra tipo Kafka/Keycloak (carpetas role=="infra")
        self.infra        = PortableInfraManager()
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
        # Backup antes de tocar el schema: si algo externo (o un bug que todavía
        # no cazamos) deja la tabla vacía después de un restart, esto da margen
        # para recuperar sin tener que re-clonar/re-escanear todo a mano. Guarda
        # como máximo 5 copias (las más viejas se van borrando).
        if os.path.isfile(self.db_path) and os.path.getsize(self.db_path) > 0:
            try:
                bdir = os.path.join(os.path.dirname(self.db_path), "backups")
                os.makedirs(bdir, exist_ok=True)
                stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                shutil.copy2(self.db_path, os.path.join(bdir, f"projects.{stamp}.db"))
                backups = sorted(Path(bdir).glob("projects.*.db"))
                for old in backups[:-5]:
                    old.unlink(missing_ok=True)
            except Exception:
                pass
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
                updated_at  TEXT,
                group_name  TEXT,
                role        TEXT,
                root_group  TEXT,
                ram_limit_mb INTEGER
            )
        """)
        # Migración para bases existentes creadas antes de group_name/role:
        # ALTER TABLE ADD COLUMN falla si la columna ya existe, así que probamos
        # y ensayamos cada una por separado en vez de chequear el schema a mano.
        for col in ("group_name TEXT", "role TEXT", "root_group TEXT", "ram_limit_mb INTEGER", "links TEXT"):
            try:
                con.execute(f"ALTER TABLE projects ADD COLUMN {col}")
            except sqlite3.OperationalError:
                pass  # ya existe
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
        for field in ("credentials", "detection", "links"):
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

        # Igual que escanear: si la carpeta trae más de un servicio arrancable
        # adentro (backend/frontend/infra, sin importar qué tan anidados estén),
        # se registran todos agrupados bajo el nombre de esta carpeta en vez de
        # registrar solo la raíz como un único proyecto.
        name = Path(path).name
        services = self._discover_services(path)
        if len(services) >= 2:
            added, skipped, errors = [], [], []
            for svc_path, role in services:
                group = self._subgroup_for(path, svc_path)
                self._add_one(svc_path, added, skipped, errors, group=group, role=role, root_group=name)
            if not added:
                if errors:
                    return {"ok": False, "error": errors[0]["error"]}
                return {"ok": False, "error": "No se detectó ningún servicio arrancable"}
            return {"ok": True, "added": added, "skipped": skipped, "errors": errors, "root_group": name}
        if len(services) == 1:
            svc_path, role = services[0]
            return self._register(svc_path, role=role)
        return self._register(path)

    # ── Alta desde Git / ZIP (para uso en web, sin ruta de disco local) ───────

    @staticmethod
    def _slug(text: str) -> str:
        s = re.sub(r"[^a-zA-Z0-9._-]+", "-", text.strip()).strip("-.")
        return s or "proyecto"

    def add_project_from_git(self, url: str, ref: str | None = None,
                             dest_folder: str | None = None) -> dict:
        """Clona un repo en <dest_folder>/<name> (o <data>/projects/<name> si no
        se especifica carpeta) y lo registra."""
        raw  = url.rstrip("/").rsplit("/", 1)[-1]
        name = self._slug(raw[:-4] if raw.endswith(".git") else raw)

        con = self._con()
        existing = con.execute("SELECT id FROM projects WHERE name = ?", (name,)).fetchone()
        con.close()
        if existing:
            return {"ok": False, "error": f"El proyecto '{name}' ya existe"}

        base = os.path.abspath(os.path.expanduser(dest_folder)) if dest_folder else projects_dir()
        if dest_folder and not os.path.isdir(base):
            return {"ok": False, "error": f"Carpeta no encontrada: {base}"}
        dest = os.path.join(base, name)
        if os.path.exists(dest):
            _rmtree(dest)

        cmd = ["git", "clone", "--depth", "1"]
        if ref:
            cmd += ["--branch", ref]
        cmd += [url, dest]
        try:
            r = subprocess.run(cmd, capture_output=True, encoding="utf-8",
                               errors="replace", timeout=300)
        except FileNotFoundError:
            return {"ok": False, "error": "git no está instalado"}
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "git clone tardó demasiado"}
        if r.returncode != 0:
            _rmtree(dest)
            return {"ok": False, "error": f"git clone falló: {(r.stderr or r.stdout or '').strip()[:400]}"}

        # Igual que scan_folder: detectar si el repo clonado trae más de un
        # servicio arrancable (microservicios) en vez de registrarlo entero
        # como un único proyecto.
        services = self._discover_services(dest)
        added, skipped, errors = [], [], []
        if len(services) >= 2:
            for svc_path, role in services:
                group = self._subgroup_for(dest, svc_path)
                self._add_one(svc_path, added, skipped, errors, group=group, role=role, root_group=name)
        elif len(services) == 1:
            svc_path, role = services[0]
            self._add_one(svc_path, added, skipped, errors, role=role, root_group=name)
        else:
            # Nada arrancable detectado: registrar igual la raíz del repo tal
            # cual (comportamiento anterior), para no perder el clone.
            return self._register(dest, name=name)

        if not added:
            _rmtree(dest)
            if errors:
                return {"ok": False, "error": errors[0]["error"]}
            return {"ok": False, "error": "No se detectó ningún servicio arrancable en el repo"}

        return {"ok": True, "added": added, "skipped": skipped, "errors": errors, "root_group": name}

    def add_project_from_zip(self, file_storage) -> dict:
        """Extrae un .zip subido a <data>/projects/<name> y lo registra."""
        base = self._slug(Path(file_storage.filename).stem)

        con = self._con()
        existing = con.execute("SELECT id FROM projects WHERE name = ?", (base,)).fetchone()
        con.close()
        if existing:
            return {"ok": False, "error": f"El proyecto '{base}' ya existe"}

        tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        try:
            file_storage.save(tmp.name)
            tmp.close()
            if not zipfile.is_zipfile(tmp.name):
                return {"ok": False, "error": "El archivo no es un .zip válido"}

            dest = os.path.join(projects_dir(), base)
            if os.path.exists(dest):
                _rmtree(dest)
            with zipfile.ZipFile(tmp.name) as z:
                z.extractall(dest)

            # Si el zip trae todo dentro de una única carpeta raíz, usar esa como root
            entries = [e for e in os.listdir(dest) if not e.startswith("__MACOSX")]
            if len(entries) == 1 and os.path.isdir(os.path.join(dest, entries[0])):
                dest = os.path.join(dest, entries[0])

            return self._register(dest, name=base, source={"kind": "zip"})
        finally:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

    def refresh_project(self, name: str) -> dict:
        """git pull sobre un proyecto registrado por Git."""
        row = self._get_row(name)
        if not row:
            return {"ok": False, "error": f"Proyecto '{name}' no encontrado"}
        path = row["path"]
        if not os.path.isdir(os.path.join(path, ".git")):
            return {"ok": False, "error": "El proyecto no es un repositorio Git"}
        try:
            r = subprocess.run(["git", "-C", path, "pull", "--ff-only"],
                               capture_output=True, encoding="utf-8",
                               errors="replace", timeout=180)
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "git pull tardó demasiado"}
        if r.returncode != 0:
            return {"ok": False, "error": f"git pull falló: {(r.stderr or r.stdout or '').strip()[:400]}"}

        # Re-detectar por si cambió la BD / forma de arrancar
        detection = DatabaseDetector(path).detect()
        con = self._con()
        con.execute("UPDATE projects SET detection = ?, updated_at = ? WHERE name = ?",
                    (json.dumps(detection), self._now(), name))
        con.commit()
        con.close()
        return {"ok": True, "output": (r.stdout or "").strip()}

    # Dónde Spring Boot suele declarar su puerto fijo (típico en servicios de
    # infra tipo Eureka/config server, que casi siempre fijan server.port en
    # vez de dejarlo en el 8080 por defecto).
    _SPRING_CONFIG_FILES = (
        "application.yml", "application.yaml", "application.properties",
        os.path.join("src", "main", "resources", "application.yml"),
        os.path.join("src", "main", "resources", "application.yaml"),
        os.path.join("src", "main", "resources", "application.properties"),
    )

    def _detect_natural_port(self, path: str, framework: str | None) -> int | None:
        """Puerto que el propio proyecto declara para arrancar (ej. server.port
        en el application.yml de un servicio Eureka). Solo lo intentamos para
        frameworks donde ese puerto suele venir fijo en el código fuente —
        Spring Boot es el caso típico. None si no se puede determinar."""
        if framework != "springboot":
            return None
        import re
        for rel in self._SPRING_CONFIG_FILES:
            fpath = os.path.join(path, rel)
            if not os.path.isfile(fpath):
                continue
            try:
                text = open(fpath, encoding="utf-8", errors="replace").read()
            except Exception:
                continue
            # application.properties: server.port=8761 (con o sin ${VAR:default})
            m = re.search(r'^\s*server\.port\s*=\s*\$\{[^:}]+:(\d+)\}', text, re.MULTILINE)
            if not m:
                m = re.search(r'^\s*server\.port\s*=\s*(\d+)\s*$', text, re.MULTILINE)
            if m:
                return int(m.group(1))
            # application.yml: bloque "server:" con "port:" indentado debajo
            m = re.search(r'^server:[ \t]*\n((?:[ \t]+.*\n?)*)', text, re.MULTILINE)
            if m:
                block = m.group(1)
                pm = re.search(r'port:\s*\$\{[^:}]+:(\d+)\}', block)
                if not pm:
                    pm = re.search(r'port:\s*["\']?(\d+)["\']?', block)
                if pm:
                    return int(pm.group(1))
        return None

    def _register(self, path: str, name: str | None = None,
                  source: dict | None = None,
                  group: str | None = None, role: str | None = None,
                  root_group: str | None = None) -> dict:
        # El nombre del proyecto es el nombre de la carpeta (salvo override de git/zip)
        name = name or Path(path).name

        con = self._con()
        existing = con.execute("SELECT id FROM projects WHERE name = ?", (name,)).fetchone()
        con.close()
        if existing:
            return {"ok": False, "error": f"El proyecto '{name}' ya existe"}

        if role == "infra":
            # Carpeta solo-infra (kafka, keycloak, zookeeper, ...): no corre
            # DatabaseDetector ni reserva puertos de app — el propio compose
            # ya define los puertos que expone cada contenedor.
            infra_info = detect_infra_service(path) or {
                "name": name, "rank": 9, "compose_file": None,
            }
            db_type   = None
            framework = "infra"
            creds     = {}
            db_port   = None
            app_port  = None
            detection = {
                "infra_name":    infra_info["name"],
                "infra_rank":    infra_info["rank"],
                "compose_file":  infra_info.get("compose_file"),
                "primary_db":    None,
                "credentials":   {},
                "app_start":     None,
            }
        else:
            # Correr el detector para encontrar qué BD usa el proyecto
            detector = DatabaseDetector(path)
            detection = detector.detect()

            db_type   = detection.get("primary_db")
            app_start = detection.get("app_start") or {}
            framework = app_start.get("type")
            # Extraer credenciales específicas de la BD primaria detectada
            creds     = detection.get("credentials", {}).get(db_type) if db_type else {}

            # Proyecto solo-frontend (React, Vue, etc.): no necesita BD ni Docker
            is_fe_only = self._has_fe(path) and not self._has_be(path)
            if is_fe_only:
                db_type = None
                creds   = {}
                framework = framework or "frontend"

            # role: si no vino explícito (ej. desde scan_folder), derivarlo de los
            # mismos indicadores que ya calculamos arriba, para que también los
            # proyectos agregados con "Agregar" (uno por uno) queden clasificados.
            if role is None:
                role = "frontend" if is_fe_only else ("backend" if self._has_be(path) else None)

            # Reservar puertos únicos para esta app y su BD. Si el proyecto declara
            # su propio puerto (ej. un servicio Eureka con server.port: 8761 en su
            # application.yml), se lo pasamos como preferido — así no colisiona en
            # el común de los casos con el que ya trae hardcodeado todo lo que
            # depende de él, y seguimos cayendo al pool genérico si ese puerto ya
            # está ocupado por otra cosa.
            natural_port = self._detect_natural_port(path, framework)
            natural_db_port = creds.get("port") if creds else None
            db_port, app_port = self.port_manager.assign_ports(
                name, natural_app_port=natural_port, natural_db_port=natural_db_port
            )

        now = self._now()
        con = self._con()
        con.execute("""
            INSERT INTO projects
              (name, path, db_type, framework, app_port, db_port, status,
               credentials, detection, created_at, updated_at, group_name, role, root_group)
            VALUES (?, ?, ?, ?, ?, ?, 'STOPPED', ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, path, db_type, framework, app_port, db_port,
            json.dumps(creds) if creds else None,
            json.dumps(detection),
            now, now, group, role, root_group,
        ))
        con.commit()
        con.close()

        return {
            "ok":        True,
            "name":      name,
            "path":      path,
            "group_name": group,
            "root_group": root_group,
            "role":      role,
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

    def _has_be(self, path: str, require_app: bool = False) -> bool:
        # Comprobar archivos típicos de BE en la raíz
        if any(os.path.exists(os.path.join(path, f)) for f in self._BE_INDICATORS):
            return True
        # docker-compose en la raíz indica que hay servicios de BD. Solo cuenta
        # como "esta carpeta ES el servicio" cuando no exigimos app propia
        # (require_app=True la ignora): en un mono-repo, compose.yml suele
        # vivir en una carpeta "infra"/"obs" que solo orquesta contenedores y
        # no es arrancable por sí misma — ignorarla ahí deja que _discover_services
        # siga bajando hasta las subcarpetas que sí tienen su propio mvnw/pom.xml.
        if not require_app:
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

    # Carpetas que nunca vale la pena bajar a explorar: dependencias instaladas,
    # salidas de build, VCS, caches. Evita recursión larga/lenta y falsos positivos
    # (ej. un package.json de una dependencia adentro de node_modules).
    _SKIP_DIRS = {
        ".git", "node_modules", "target", "build", "dist", "vendor",
        "venv", ".venv", "__pycache__", ".mvn", ".idea", ".vscode",
        "bin", "obj", ".gradle", ".next", ".nuxt", "coverage", ".pytest_cache",
    }

    def _discover_services(self, path: str, depth: int = 0, max_depth: int = 5) -> list:
        """
        Recorre `path` buscando sub-proyectos arrancables (backend o frontend),
        sin importar qué tan anidados estén (monolito en la raíz, backend/+frontend/
        a un nivel, o microservicios tipo services/<nombre>-ms/ más abajo).
        Devuelve [(ruta, role)] — 'backend' o 'frontend'. Una vez que una carpeta
        califica como servicio, no seguimos bajando dentro de ella (evita registrar
        un microservicio Y, de paso, carpetas internas suyas que also matcheen).
        """
        is_be = self._has_be(path, require_app=True)
        is_fe = self._has_fe(path)
        if is_be or is_fe:
            return [(path, "backend" if is_be else "frontend")]
        # Carpeta "solo infra" (kafka, keycloak, zookeeper, rabbitmq, ...):
        # trae docker-compose pero no código de app propio. Se registra como
        # servicio leaf igual que backend/frontend, sin seguir bajando adentro.
        if detect_infra_service(path):
            return [(path, "infra")]
        if depth >= max_depth:
            return []
        try:
            sub_entries = [e for e in os.scandir(path)
                           if e.is_dir() and not e.name.startswith(".")
                           and e.name not in self._SKIP_DIRS]
        except (PermissionError, FileNotFoundError):
            return []
        found = []
        for e in sorted(sub_entries, key=lambda x: x.name):
            found.extend(self._discover_services(e.path, depth + 1, max_depth))
        return found

    @staticmethod
    def _subgroup_for(root_path: str, svc_path: str) -> str | None:
        """
        Nombre del subgrupo visual para `svc_path` dentro de `root_path`: la
        primera carpeta intermedia real (ej. "services", "infra") cuando el
        servicio está anidado más abajo, o None si el servicio es una carpeta
        de primer nivel de root_path (va suelto bajo el root, sin subgrupo).
        Así la UI refleja la carpeta real que agrupa los microservicios
        (monorepo por capas tipo services/+infra/) en vez de aplanar todo.
        """
        if os.path.dirname(svc_path) == root_path.rstrip(os.sep):
            return None
        rel = os.path.relpath(svc_path, root_path)
        return rel.split(os.sep)[0]

    def _add_one(self, path: str, added: list, skipped: list, errors: list,
                 group: str | None = None, role: str | None = None,
                 root_group: str | None = None):
        name = Path(path).name
        result = self._register(path, group=group, role=role, root_group=root_group)
        if result.get("ok"):
            added.append(result)
        elif "ya existe" in result.get("error", ""):
            skipped.append(name)
        else:
            errors.append({"name": name, "error": result.get("error")})

    def scan_folder(self, folder: str) -> dict:
        """
        Escanea una carpeta y registra automáticamente todo lo que encuentre
        arrancable adentro, sin importar la profundidad. Cada entrada de primer
        nivel de `folder` se trata como un posible mono-repo: si adentro aparece
        más de un servicio (backend y/o frontend, a cualquier nivel), se registran
        todos bajo el mismo group_name (el nombre de esa entrada de primer nivel)
        para poder arrancarlos juntos después con start_group(). Si solo aparece
        uno, se registra suelto (igual que antes: sin agrupar).
        """
        folder = os.path.abspath(os.path.expanduser(folder))
        if not os.path.isdir(folder):
            return {"ok": False, "error": f"Carpeta no encontrada: {folder}"}

        added, skipped, errors = [], [], []
        try:
            entries = sorted(os.scandir(folder), key=lambda e: e.name)
        except PermissionError as e:
            return {"ok": False, "error": str(e)}

        # Nombre de la carpeta raíz escaneada (ej. "pruebita_garrita_xd"): se
        # guarda en cada servicio registrado como root_group para que la UI
        # pueda anidar los sub-grupos (infra, services, ...) bajo un único
        # nodo con el nombre del mono-repo, en vez de mostrarlos sueltos.
        root_name = Path(folder).name

        for entry in entries:
            if (not entry.is_dir() or entry.name.startswith(".")
                    or entry.name in self._SKIP_DIRS):
                continue

            services = self._discover_services(entry.path)
            if len(services) >= 2:
                group = entry.name
                for svc_path, role in services:
                    self._add_one(svc_path, added, skipped, errors, group=group, role=role,
                                  root_group=root_name)
            elif len(services) == 1:
                svc_path, role = services[0]
                self._add_one(svc_path, added, skipped, errors, role=role, root_group=root_name)
            else:
                skipped.append(entry.name)

        return {"ok": True, "added": added, "skipped": skipped, "errors": errors}

    # ── Grupos (mono-repo) ───────────────────────────────────────────────────

    # Palabras en el path que sugieren "infraestructura compartida" (config
    # server, service discovery, gateway) dentro de un grupo de microservicios.
    # Arrancarlas primero les da ventaja de tiempo antes que los servicios que
    # típicamente dependen de ellas al boot. Es una heurística de orden nomás,
    # no una garantía de que ya estén listas para recibir conexiones.
    _INFRA_HINTS = ("config", "discovery", "eureka", "registry", "gateway", "consul")

    def _group_infra_urls(self, group: str, exclude_name: str, root_group: str | None = None) -> dict:
        """URLs reales (localhost:<puerto reasignado>) de los servicios de
        infraestructura (eureka, config server) del grupo, si ya están corriendo.
        Los puertos se reasignan por proyecto (ver PortManager) para que varios
        proyectos puedan convivir sin chocar, así que el puerto "de fábrica" que
        cada microservicio trae hardcodeado en su application.yml (ej. eureka en
        8761, config server en el puerto que sea) casi nunca es el real una vez
        levantado acá — sin esto, los servicios intentan descubrirse entre sí en
        el puerto equivocado y Eureka queda con "No instances available" aunque
        todo haya arrancado bien.

        Si el proyecto tiene root_group (mono-repo escaneado con sub-grupos tipo
        infra/ y services/), buscamos en TODO el root_group, no solo en el
        group_name puntual — si no, un servicio en "services" nunca encuentra a
        Eureka/el config server, que viven en el sub-grupo "infra" de al lado."""
        if not group and not root_group:
            return {}
        urls: dict = {}
        for p in self.list_projects():
            same_scope = (p.get("root_group") == root_group) if root_group else (p.get("group_name") == group)
            if not same_scope or p.get("name") == exclude_name:
                continue
            if p.get("status") != "RUNNING" or not p.get("app_port"):
                continue
            key = (p.get("path") or p.get("name") or "").lower()
            if "eureka" in key and "eureka" not in urls:
                urls["eureka"] = f"http://localhost:{p['app_port']}/eureka/"
            elif "config" in key and "config" not in urls:
                urls["config"] = f"http://localhost:{p['app_port']}"
        return urls

    def list_groups(self) -> dict:
        """{group_name: [proyectos...]} para todos los proyectos agrupados."""
        groups: dict = {}
        for p in self.list_projects():
            g = p.get("group_name")
            if g:
                groups.setdefault(g, []).append(p)
        return groups

    def start_group(self, group: str) -> dict:
        """
        Arranca todos los proyectos de un grupo: primero los backend (infra-like
        primero dentro de ese subconjunto), después los frontend. Devuelve el
        resultado de cada uno y la lista de URLs ya listas para abrir.
        """
        con = self._con()
        rows = con.execute(
            "SELECT * FROM projects WHERE group_name = ? OR root_group = ? ORDER BY name", (group, group)
        ).fetchall()
        con.close()
        projects = [self._row_to_dict(r) for r in rows]
        if not projects:
            return {"ok": False, "error": f"No hay proyectos en el grupo '{group}'"}

        # Grupos grandes (muchos java/postgres arrancando casi a la vez) pueden
        # llevar la RAM al límite y hacer que el reconcile de credenciales de
        # Postgres falle por falta de recursos momentánea, no por auth real
        # (ver _pg_reconcile_and_create). Liberar RAM antes de arrancar reduce
        # la chance de llegar a ese punto. Best-effort: si falla (no es
        # Windows, no es Administrador, etc.) seguimos igual.
        if len(projects) >= 5:
            try:
                from core.mem_tools import free_ram
                free_ram()
            except Exception:
                pass

        def _infra_rank(p):
            path_lower = (p.get("path") or "").lower()
            return 0 if any(h in path_lower for h in self._INFRA_HINTS) else 1

        def _infra_svc_rank(p):
            return (p.get("detection") or {}).get("infra_rank", 9)

        # Infra "pura" (kafka, zookeeper, keycloak, ...) va primero y en orden
        # de dependencia interna (zookeeper antes que kafka, etc.): los backends
        # típicamente la necesitan arriba antes de poder conectarse al boot.
        infra     = sorted([p for p in projects if p.get("role") == "infra"],
                           key=lambda p: (_infra_svc_rank(p), p["name"]))
        backends  = sorted([p for p in projects if p.get("role") == "backend"],
                           key=lambda p: (_infra_rank(p), p["name"]))
        frontends = sorted([p for p in projects if p.get("role") == "frontend"],
                           key=lambda p: p["name"])
        others    = [p for p in projects if p.get("role") not in ("backend", "frontend", "infra")]

        results = []
        ordered = infra + backends + frontends + others
        for p in ordered:
            if p.get("status") == "RUNNING":
                results.append({"name": p["name"], "ok": True, "already": True})
                continue
            # Chequeo en vivo durante el arranque del grupo: si entre un
            # servicio y el siguiente la RAM ya pasó el 70%, liberar antes de
            # seguir — evita que el próximo java/postgres no consiga memoria
            # para inicializarse. Best-effort, igual que el free_ram de arriba.
            try:
                from core.mem_tools import mem_load_percent, free_ram
                if mem_load_percent() >= 70:
                    free_ram()
            except Exception:
                pass
            res = self.up(p["name"])
            results.append({
                "name": p["name"], "ok": res.get("ok", False), "error": res.get("error"),
                "port_conflict": res.get("port_conflict"),
            })
            if p.get("role") in ("backend", "infra"):
                # Margen corto para que el backend/infra recién prendido tenga chance
                # de terminar su boot (conectar a config server, registrarse en
                # discovery, etc.) antes de prender el siguiente. No es un
                # healthcheck real: solo evita que todo arranque en el mismo instante.
                time.sleep(2)

        urls = []
        for p in ordered:
            updated = self._get_row(p["name"])
            if updated.get("status") == "RUNNING" and updated.get("app_port"):
                urls.append({
                    "name": updated["name"],
                    "role": updated.get("role"),
                    "url":  f"http://localhost:{updated['app_port']}",
                })

        ok = all(r["ok"] for r in results)
        return {"ok": ok, "results": results, "urls": urls}

    def stop_group(self, group: str) -> dict:
        """Baja todos los proyectos de un grupo (o de un root_group entero,
        mismo matching que start_group). Orden inverso a start_group: frontend
        primero, infra al final — así no se corta un backend mientras otro
        servicio todavía lo está usando activamente."""
        con = self._con()
        rows = con.execute(
            "SELECT * FROM projects WHERE group_name = ? OR root_group = ? ORDER BY name", (group, group)
        ).fetchall()
        con.close()
        projects = [self._row_to_dict(r) for r in rows]
        if not projects:
            return {"ok": False, "error": f"No hay proyectos en '{group}'"}

        def _infra_rank(p):
            path_lower = (p.get("path") or "").lower()
            return 0 if any(h in path_lower for h in self._INFRA_HINTS) else 1

        def _infra_svc_rank(p):
            return (p.get("detection") or {}).get("infra_rank", 9)

        infra     = sorted([p for p in projects if p.get("role") == "infra"],
                           key=lambda p: (_infra_svc_rank(p), p["name"]), reverse=True)
        backends  = sorted([p for p in projects if p.get("role") == "backend"],
                           key=lambda p: (_infra_rank(p), p["name"]), reverse=True)
        frontends = sorted([p for p in projects if p.get("role") == "frontend"],
                           key=lambda p: p["name"])
        others    = [p for p in projects if p.get("role") not in ("backend", "frontend", "infra")]

        results = []
        for p in frontends + backends + infra + others:
            if p.get("status") not in ("RUNNING", "STARTING"):
                results.append({"name": p["name"], "ok": True, "already": True})
                continue
            res = self.down(p["name"])
            results.append({"name": p["name"], "ok": res.get("ok", False), "error": res.get("error")})

        ok = all(r["ok"] for r in results)
        return {"ok": ok, "results": results}

    def restart_group(self, group: str) -> dict:
        stop_res = self.stop_group(group)
        if not stop_res.get("ok") and "error" in stop_res:
            return stop_res
        return self.start_group(group)

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

        # Si el proyecto fue clonado/extraído por nosotros (vive bajo projects_dir),
        # borrar la copia en disco. Los proyectos con ruta propia del usuario no se tocan.
        self._rmtree_if_managed(row.get("path", ""))

        return {"ok": True}

    def _rmtree_if_managed(self, path: str):
        """Borra en disco el hijo directo de projects_dir() que contiene `path`,
        solo si `path` vive bajo nuestra carpeta interna (clonado/extraído por
        nosotros). Las rutas propias del usuario nunca se tocan.

        Antes de borrar, chequea que ningún OTRO proyecto siga registrado bajo
        esa misma carpeta de primer nivel (ej. un mono-repo escaneado donde
        "infra"/"services" quedaron registrados como proyectos aparte, colgando
        de la misma carpeta raíz clonada). Si alguno sigue vivo, no se borra —
        si no, borrar el "proyecto raíz" huérfano después de escanear sus
        subcarpetas se llevaba puesto los servicios recién registrados."""
        try:
            proot = os.path.abspath(projects_dir())
            ppath = os.path.abspath(path)
            if ppath == proot or ppath.startswith(proot + os.sep):
                rel = os.path.relpath(ppath, proot).split(os.sep)[0]
                if rel and rel not in (".", ".."):
                    folder = os.path.join(proot, rel)
                    con = self._con()
                    still_used = con.execute(
                        "SELECT 1 FROM projects WHERE path = ? OR path LIKE ? LIMIT 1",
                        (folder, folder + os.sep + "%")
                    ).fetchone()
                    con.close()
                    if still_used:
                        return
                    _rmtree(folder)
        except Exception:
            pass

    def remove_group(self, group: str) -> dict:
        """Elimina todos los proyectos de un grupo (mono-repo escaneado) de una
        sola vez. Todos sus miembros viven bajo la misma carpeta raíz clonada/
        escaneada, así que el borrado en disco se hace una única vez al final
        (si cada miembro lo hiciera por su cuenta, el primero ya borraría la
        carpeta entera y dejaría a los demás apuntando a rutas inexistentes)."""
        con = self._con()
        rows = con.execute(
            "SELECT * FROM projects WHERE group_name = ?", (group,)
        ).fetchall()
        con.close()
        projects = [self._row_to_dict(r) for r in rows]
        if not projects:
            return {"ok": False, "error": f"No hay proyectos en el grupo '{group}'"}

        for p in projects:
            name = p["name"]
            if p.get("status") in ("RUNNING", "STARTING"):
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

        self._rmtree_if_managed(projects[0].get("path", ""))
        return {"ok": True, "removed": [p["name"] for p in projects]}

    def remove_root_group(self, root_group: str) -> dict:
        """Elimina todos los proyectos de un mono-repo escaneado (todos los
        sub-grupos + servicios sueltos que comparten el mismo root_group) de
        una sola vez. Mismo cuidado que remove_group: el borrado en disco se
        hace una única vez al final, no por cada miembro."""
        con = self._con()
        rows = con.execute(
            "SELECT * FROM projects WHERE root_group = ?", (root_group,)
        ).fetchall()
        con.close()
        projects = [self._row_to_dict(r) for r in rows]
        if not projects:
            return {"ok": False, "error": f"No hay proyectos en '{root_group}'"}

        for p in projects:
            name = p["name"]
            if p.get("status") in ("RUNNING", "STARTING"):
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

        self._rmtree_if_managed(projects[0].get("path", ""))
        return {"ok": True, "removed": [p["name"] for p in projects]}

    def set_ram_limit(self, name: str, mb: int | None) -> dict:
        """Fija (o limpia, con mb=None) el heap máximo de la JVM para este
        proyecto. Solo aplica al próximo arranque — si está corriendo, hay
        que reiniciarlo para que tome efecto."""
        row = self._get_row(name)
        if not row:
            return {"ok": False, "error": f"Proyecto '{name}' no encontrado"}
        con = self._con()
        con.execute("UPDATE projects SET ram_limit_mb = ?, updated_at = ? WHERE name = ?",
                    (mb, self._now(), name))
        con.commit()
        con.close()
        return {"ok": True, "ram_limit_mb": mb}

    def set_group_ram_limit(self, group: str, mb: int | None) -> dict:
        con = self._con()
        names = [r["name"] for r in con.execute(
            "SELECT name FROM projects WHERE group_name = ? OR root_group = ?", (group, group)
        ).fetchall()]
        if not names:
            con.close()
            return {"ok": False, "error": f"No hay proyectos en '{group}'"}
        now = self._now()
        con.executemany("UPDATE projects SET ram_limit_mb = ?, updated_at = ? WHERE name = ?",
                        [(mb, now, n) for n in names])
        con.commit()
        con.close()
        return {"ok": True, "updated": names, "ram_limit_mb": mb}

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

    @staticmethod
    def _port_is_alive(port: int, timeout: float = 1.0) -> bool:
        """True si algo acepta conexiones TCP en localhost:port ahora mismo.
        Un connect exitoso alcanza — no importa qué responda la app por encima
        (hasta un 404 cuenta: significa que el servidor HTTP ya está arriba)."""
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=timeout):
                return True
        except OSError:
            return False

    def _port_owner(self, port: int, exclude_name: str | None = None) -> dict | None:
        """Quién tiene agarrado `port`. Primero busca entre nuestros propios
        proyectos (el caso más común y el más accionable: se puede bajar con
        un clic); si no es ninguno de los nuestros, identifica el proceso del
        sistema operativo que lo tiene (PID + nombre) para que el usuario
        decida si lo mata él mismo."""
        for p in self.list_projects():
            if p.get("name") == exclude_name:
                continue
            if p.get("status") == "RUNNING" and (
                p.get("app_port") == port or p.get("db_port") == port
            ):
                return {"port": port, "project": p["name"], "pid": None, "process": None}

        pid, process = self._os_port_owner(port)
        if pid is None:
            return None  # el puerto se liberó justo entre el chequeo y ahora
        return {"port": port, "project": None, "pid": pid, "process": process}

    @staticmethod
    def _os_port_owner(port: int) -> "tuple[int | None, str | None]":
        try:
            if _IS_WINDOWS:
                out = subprocess.run(
                    ["netstat", "-ano", "-p", "TCP"],
                    capture_output=True, encoding="utf-8", errors="replace", timeout=5
                ).stdout or ""
                pid = None
                for line in out.splitlines():
                    parts = line.split()
                    if len(parts) >= 5 and parts[0] == "TCP" and parts[-1].isdigit():
                        local = parts[1]
                        if local.rsplit(":", 1)[-1] == str(port) and "LISTENING" in line:
                            pid = int(parts[-1])
                            break
                if pid is None:
                    return None, None
                r = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                    capture_output=True, encoding="utf-8", errors="replace", timeout=5
                )
                name = None
                line = (r.stdout or "").strip()
                if line and line.startswith('"'):
                    name = line.split('","')[0].strip('"')
                return pid, name
            else:
                out = subprocess.run(
                    ["lsof", "-i", f":{port}", "-sTCP:LISTEN", "-t"],
                    capture_output=True, encoding="utf-8", errors="replace", timeout=5
                ).stdout or ""
                pid_str = out.strip().splitlines()[0] if out.strip() else ""
                if not pid_str.isdigit():
                    return None, None
                pid = int(pid_str)
                r = subprocess.run(["ps", "-p", str(pid), "-o", "comm="],
                                    capture_output=True, encoding="utf-8", errors="replace", timeout=5)
                return pid, (r.stdout or "").strip() or None
        except Exception:
            return None, None

    def _up_infra(self, row: dict) -> dict:
        """Arranca una carpeta solo-infra (kafka, keycloak, zookeeper, ...).
        Si el tipo tiene arranque nativo (Kafka, Keycloak), corre como proceso
        portable sin Docker. Si no (ej. zookeeper, rabbitmq, obs todavía no
        tienen soporte nativo), cae a Docker si está disponible."""
        name = row["name"]
        path = row["path"]
        infra_name = (row.get("detection") or {}).get("infra_name", name)

        self._set_status(name, "STARTING")

        if infra_name in self.infra.SUPPORTED:
            result = self.infra.up(infra_name, path, on_log=lambda line: self.append_log(name, line))
            if not result.get("ok"):
                self._set_status(name, "ERROR")
                return result
            self._set_status(name, "RUNNING")
            port = result.get("port")
            app_url = f"http://localhost:{port}" if port else None
            # Bundles (obs, kafka) arrancan varios sub-servicios con UI propia
            # (grafana/prometheus/loki, kafbat). En vez de un solo link al "principal",
            # guardamos uno por sub-servicio para que la UI los muestre aparte.
            links = [
                {"name": p["name"], "url": f"http://localhost:{p['port']}"}
                for p in (result.get("procs") or [])
                if p.get("ok") and p.get("port") and p["name"] in _BROWSABLE_INFRA_SERVICES
            ]
            con = self._con()
            con.execute(
                "UPDATE projects SET app_port = ?, links = ?, updated_at = ? WHERE name = ?",
                (port, json.dumps(links) if links else None, self._now(), name)
            )
            con.commit()
            con.close()
            return {"ok": True, "app_url": app_url, "app_port": port, "db_port": None, "links": links}

        if isinstance(self.docker, DockerManager):
            self.docker._progress_logger = lambda line: self.append_log(name, line)
            result = self.docker.up(path, f"orq_{name}_net", {})
            if not result.get("ok"):
                self._set_status(name, "ERROR")
                return result
            self._set_status(name, "RUNNING")
            return {"ok": True, "app_url": None, "app_port": None, "db_port": None}

        self._set_status(name, "ERROR")
        return {"ok": False,
                "error": f"'{infra_name}' todavía no tiene arranque nativo "
                         f"(soportados: {', '.join(self.infra.SUPPORTED)}) "
                         f"y Docker no está disponible como fallback"}

    def up(self, name: str) -> dict:
        row = self._get_row(name)
        if not row:
            return {"ok": False, "error": f"Proyecto '{name}' no encontrado"}
        if row.get("role") == "infra":
            return self._up_infra(row)

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

        # Si el puerto de la app ya está ocupado por OTRA cosa (no el proceso
        # anterior de este mismo proyecto, que ya se limpió arriba), no seguimos
        # de largo: arrancaríamos igual pero fallaría más abajo con un mensaje
        # críptico de "port already in use" en el propio log de la app. Mejor
        # decir desde ya quién lo tiene — puede ser otro de nuestros proyectos
        # (se puede bajar con un clic) o un proceso suelto del sistema.
        app_port_preflight = row.get("app_port")
        if app_port_preflight and not self.port_manager.is_port_free(app_port_preflight):
            # Antes de declarar conflicto: si algo responde de verdad en ese puerto
            # (típicamente la app de ESTE mismo proyecto, ya arrancada en un up()
            # anterior cuyo estado en la BD quedó desincronizado — p.ej. un intento
            # posterior que falló por otra razón y pisó el status a ERROR aunque el
            # proceso real siguiera vivo y sano), no tiene sentido fallar: el
            # servicio ya está disponible. Evita falsos "ERROR" con el puerto activo.
            if self._port_is_alive(app_port_preflight):
                self._set_status(name, "RUNNING")
                updated = self._get_row(name)
                return {
                    "ok": True,
                    "already_running": True,
                    "app_url": f"http://localhost:{app_port_preflight}",
                    "app_port": app_port_preflight,
                    "db_port": updated.get("db_port"),
                }
            owner = self._port_owner(app_port_preflight, exclude_name=name)
            if owner:
                self._set_status(name, "ERROR")
                return {"ok": False, "error": f"Puerto {app_port_preflight} ocupado", "port_conflict": owner}

        self._set_status(name, "STARTING")
        path    = row["path"]
        db_type = row.get("db_type")
        creds   = row.get("credentials") or {}
        db_port = row.get("db_port")

        # Proyectos frontend-only no necesitan Docker ni BD
        is_frontend_only = not db_type

        detection = row.get("detection") or {}
        compose_file = detection.get("compose_file")

        # Si no hay compose.yml existente, generarlo desde la plantilla para la BD detectada
        if not is_frontend_only:
            if not compose_file or not os.path.isfile(compose_file):
                if db_type:
                    compose_file = self.docker.generate_compose(
                        path, db_type, creds, {"db_port": db_port}
                    )

        # Si el compose.yml bundlea BD + app (build: .) y hay forma de arrancar
        # la app en nativo (mvnw, npm, etc.), levantamos SOLO el servicio de la
        # BD por Docker — la app la arranca _start_app() más abajo, igual que
        # el usuario hace a mano desde el IDE (./mvnw.cmd spring-boot:run).
        # Si no, Docker levantaría también el contenedor de la app (build
        # redundante, y si el healthcheck de la BD tarda, falla el `up` entero).
        only_db_service = None
        app_start_detected = detection.get("app_start") or {}
        if (not is_frontend_only and compose_file and os.path.isfile(compose_file)
                and app_start_detected.get("cmd") and hasattr(self.docker, "find_db_service_name")):
            try:
                with open(compose_file, encoding="utf-8", errors="replace") as f:
                    compose_content = f.read()
            except Exception:
                compose_content = ""
            if "build:" in compose_content:
                only_db_service = self.docker.find_db_service_name(compose_file, db_type or "")

        # Levantar Docker Compose (o PortableManager si no hay Docker)
        if not is_frontend_only and compose_file and os.path.isfile(compose_file):
            # Si self.docker es PortableManager y necesita descargar binarios
            # (primera vez con esa BD), esto le da por dónde avisar progreso —
            # PortableManager.up() lo usa si existe, lo ignora si no (p.ej. con
            # Docker real, que no lo necesita).
            self.docker._progress_logger = lambda line: self.append_log(name, line)
            result = self.docker.up(path, f"orq_{name}_net", {"db_port": db_port},
                                     only_service=only_db_service)
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
        app_start  = detection.get("app_start") or {}
        app_port   = row.get("app_port")
        infra_urls = self._group_infra_urls(row.get("group_name"), name, root_group=row.get("root_group"))
        app_proc   = self._start_app(name, app_start, creds, db_type, app_port, infra_urls,
                                     ram_limit_mb=row.get("ram_limit_mb"))
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
                   app_port: int = None, infra_urls: dict | None = None,
                   ram_limit_mb: int | None = None) -> "subprocess.Popen | None":
        # Si el detector no encontró cómo arrancar la app, no hacemos nada
        if not app_start or not app_start.get("cmd"):
            return None

        cmd = app_start["cmd"]
        cwd = app_start.get("cwd", ".")
        if _IS_WINDOWS:
            # cmd.exe (usado por subprocess shell=True) no busca el directorio
            # actual al resolver un ejecutable dado por nombre pelado (ej. "mvnw.cmd"),
            # a diferencia del viejo comportamiento DOS — en este sistema falla con
            # "'mvnw.cmd' no se reconoce como un comando interno o externo" y el
            # proceso nunca llega a lanzar java. Si el primer token del comando es
            # un script que existe tal cual en cwd, forzamos la ruta explícita ".\".
            parts = cmd.split(None, 1)
            if parts and not parts[0].startswith((".\\", "./", "\\")) and ":" not in parts[0][:2]:
                candidate = os.path.join(cwd, parts[0])
                if os.path.isfile(candidate):
                    rest = parts[1] if len(parts) > 1 else ""
                    cmd = f".\\{parts[0]}" + (f" {rest}" if rest else "")
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
            # Modo nativo: sin Docker, los hostnames internos de compose
            # (lamb-kafka, lamb-keycloak, lamb-eureka...) no resuelven. Forzamos
            # el perfil "dev" — si el proyecto trae un application-dev.yml (patrón
            # típico en monorepos con compose-dev.yml paralelo), ese perfil ya
            # apunta a localhost con los mismos puertos que la infra nativa expone.
            # Solo forzamos "dev" si el proyecto de verdad trae un
            # application-dev.(yml|yaml|properties) — si no, puede ser un
            # servicio con su propio perfil default (ej. lamb-config usa
            # "native" para elegir su backend de config server basado en
            # archivos; pisarlo con "dev" rompe la selección de ese backend
            # y tira "You need to configure a uri for the git repository").
            _dev_profile_files = (
                "application-dev.yml", "application-dev.yaml", "application-dev.properties",
            )
            _res_dirs = (
                os.path.join(cwd, "src", "main", "resources"),
                cwd,
            )
            if "SPRING_PROFILES_ACTIVE" not in env and any(
                os.path.isfile(os.path.join(d, f)) for d in _res_dirs for f in _dev_profile_files
            ):
                env["SPRING_PROFILES_ACTIVE"] = "dev"
            # Service discovery / config server del mismo grupo: los services
            # traen hardcodeado el puerto "de fábrica" (ej. eureka en 8761) en
            # su application.yml, pero acá cada proyecto corre en un puerto
            # reasignado — sin esto, un microservicio nunca encuentra a su
            # eureka/config server real aunque ambos estén arriba.
            infra_urls = infra_urls or {}
            jvm_opts = []
            if infra_urls.get("eureka"):
                # OJO: "eureka.client.serviceUrl.defaultZone" es una clave de un
                # Map<String,String>, no una propiedad de bean normal — el binding
                # relajado de variables de entorno de Spring Boot NO reconstruye
                # bien el camelCase "defaultZone" ahí (termina sin efecto, Eureka
                # se queda con su 8761 de fábrica en silencio, sin error). Por eso
                # esto va como system property (-D) en vez de env var: ahí el
                # nombre exacto se respeta tal cual.
                jvm_opts.append(f'-Deureka.client.serviceUrl.defaultZone={infra_urls["eureka"]}')
            if infra_urls.get("config"):
                env["SPRING_CONFIG_IMPORT"] = f"optional:configserver:{infra_urls['config']}"
            # Límite de heap opcional (botón "Ahorro RAM" en la UI): sin esto,
            # cada JVM negocia su propio heap contra la RAM total de la máquina
            # (hasta 25% cada una por default), lo que se dispara feo al tener
            # varias arrancadas al mismo tiempo.
            if ram_limit_mb:
                jvm_opts.append(f"-Xmx{ram_limit_mb}m")
            # java.io.tmpdir propio por servicio, recreado en cada arranque:
            # Spring Boot valida que el dueño de su carpeta temp coincida con
            # el usuario actual, y un proceso elevado por UAC crea archivos
            # como BUILTIN\Administradores en vez de tu usuario. Si reusamos
            # el %TEMP% global, correr el server a veces admin / a veces no
            # deja carpetas con dueño viejo y Spring se niega a arrancar
            # ("Existing directory ... is not owned by ..."). Al tener una
            # carpeta dedicada y borrarla antes de cada start, el dueño
            # siempre es quien la está creando ahora mismo — sin choque.
            tmp_dir = os.path.join(data_root(), "javatmp", name)
            try:
                if os.path.isdir(tmp_dir):
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                os.makedirs(tmp_dir, exist_ok=True)
                jvm_opts.append(f'-Djava.io.tmpdir={tmp_dir}')
            except Exception:
                pass
            if jvm_opts:
                env["JAVA_TOOL_OPTIONS"] = " ".join(jvm_opts)
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

    def _find_pid_by_port(self, port: int) -> int | None:
        if not _IS_WINDOWS:
            return None
        try:
            out = subprocess.run(
                ["netstat", "-ano"], capture_output=True, text=True, timeout=10
            ).stdout
            for line in out.splitlines():
                parts = line.split()
                if len(parts) < 5 or parts[0] not in ("TCP", "TCPv6"):
                    continue
                if parts[-2] != "LISTENING":
                    continue
                local = parts[1]
                if local.rsplit(":", 1)[-1] == str(port):
                    return int(parts[-1])
        except Exception:
            pass
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
                if _IS_WINDOWS:
                    # El proceso lanzado es "cmd.exe /c <cmd>" (shell=True). Para
                    # comandos tipo "npm run start", npm.cmd genera a su vez un
                    # node.exe hijo; proc.terminate() solo mata el cmd.exe de arriba
                    # y deja a node.exe huérfano, corriendo (y reteniendo el puerto)
                    # para siempre. taskkill /T mata todo el árbol por PID.
                    subprocess.run(
                        ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                        capture_output=True, timeout=10
                    )
                else:
                    proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        elif _IS_WINDOWS and row.get("app_port"):
            # self._procs es un dict en memoria: si el server se reinició
            # (crash, redeploy, lo que sea) perdemos el handle aunque el
            # proceso real siga vivo — down() se convertía en un no-op que
            # solo mentía en la DB ("STOPPED") dejando el java/node huérfano
            # para siempre. Fallback: buscamos quién tiene el puerto abierto
            # y lo matamos directo por PID.
            pid = self._find_pid_by_port(row["app_port"])
            if pid:
                try:
                    subprocess.run(
                        ["taskkill", "/T", "/F", "/PID", str(pid)],
                        capture_output=True, timeout=10
                    )
                except Exception:
                    pass

        # Bajar contenedores Docker (o detener proceso nativo)
        if row.get("role") == "infra":
            infra_name = (row.get("detection") or {}).get("infra_name", name)
            if infra_name in self.infra.SUPPORTED:
                self.infra.down(infra_name, row["path"])
            elif isinstance(self.docker, DockerManager):
                self.docker.down(row["path"])
        else:
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

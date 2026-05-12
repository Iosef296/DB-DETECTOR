import os
import re
import json
import glob
import platform
from pathlib import Path
from typing import Optional

_IS_WINDOWS = platform.system() == "Windows"

# ─── Patrones de detección ────────────────────────────────────────────────────

DB_URL_PATTERNS = {
    "postgresql": [r"postgres(?:ql)?://([^:\s]+):([^@\s]+)@([^:/\s]+)(?::(\d+))?/(\S+)"],
    "mysql":      [
        r"mysql(?:\+\w+)?://([^:\s]+):([^@\s]+)@([^:/\s]+)(?::(\d+))?/(\S+)",
        # JDBC format: jdbc:mysql://host:port/db
        r"jdbc:mysql://([^:\s/]*)(?::(\d+))?/([^\s?\"']+)",
    ],
    "mongodb":    [r"mongodb(?:\+srv)?://(?:([^:\s]+):([^@\s]+)@)?([^:/\s,]+)(?::(\d+))?/([^\s\"\')\]},]+)?"],
    "sqlite":     [r"sqlite(?::\/{2,3})(.+)"],
    "redis":      [r"redis://(?:([^:\s]*):([^@\s]*)@)?([^:/\s]+)(?::(\d+))?(?:/(\d+))?"],
}

ENV_KEY_PATTERNS = {
    "postgresql": ["DATABASE_URL", "POSTGRES_URL", "PG_URL", "POSTGRESQL_URL",
                   "DB_URL", "POSTGRES_URI", "PGDATABASE", "POSTGRES_DB"],
    "mysql":      ["MYSQL_URL", "MYSQL_URI", "DATABASE_URL", "DB_URL",
                   "MYSQL_DATABASE", "MYSQL_HOST"],
    "mongodb":    ["MONGODB_URI", "MONGO_URL", "MONGODB_URL", "MONGO_URI",
                   "DATABASE_URL", "MONGO_DATABASE"],
    "sqlite":     ["DATABASE_URL", "SQLITE_PATH", "DB_PATH"],
    "redis":      ["REDIS_URL", "REDIS_URI", "REDIS_HOST", "CACHE_URL"],
}

DRIVER_PATTERNS = {
    "postgresql": ["psycopg2", "psycopg", "pg", "postgres", "asyncpg",
                   "pg2", "node-postgres", "typeorm", "sequelize", "sqlalchemy",
                   "tortoise-orm", "databases", "aiopg"],
    "mysql":      ["mysql2", "mysql", "pymysql", "aiomysql", "mysqlclient",
                   "mysql-connector", "mariadb"],
    "mongodb":    ["mongoose", "pymongo", "motor", "mongodb", "mongoengine",
                   "mongo", "beanie"],
    "sqlite":     ["sqlite3", "better-sqlite3", "sql.js", "aiosqlite",
                   "sqlite", "typeorm"],
    "redis":      ["redis", "ioredis", "aioredis", "hiredis", "redis-py",
                   "fakeredis"],
}

COMPOSE_IMAGE_PATTERNS = {
    "postgresql": ["postgres", "postgresql", "postgis"],
    "mysql":      ["mysql", "mariadb", "percona"],
    "mongodb":    ["mongo", "mongodb"],
    "redis":      ["redis"],
    "sqlite":     [],
}

# ─── Archivos de dependencias ─────────────────────────────────────────────────

DEP_FILES = [
    "package.json", "package-lock.json",
    "requirements.txt", "Pipfile", "pyproject.toml", "setup.py", "setup.cfg",
    "Gemfile", "go.mod", "go.sum",
    "pom.xml", "build.gradle", "build.gradle.kts",
    "composer.json",
    "*.csproj", "*.fsproj", "packages.config",
    "Cargo.toml",
]

ENV_FILES = [
    # Archivos .env estándar
    ".env", ".env.local", ".env.development", ".env.production",
    ".env.staging", ".env.test", ".env.prod", ".env.cloud",
    ".env.example", ".env.sample", "config.env",
    # Plataformas cloud
    "render.yaml", "render.yml",
    "fly.toml",
    "railway.toml", "railway.json",
    "app.json",                          # Heroku
    "netlify.toml",
    "vercel.json",
    ".supabase/config.toml",
    # Docker
    "docker-compose.yml", "docker-compose.yaml",
    "docker-compose.override.yml",
    "docker-compose.prod.yml", "docker-compose.production.yml",
    # Frameworks
    "app.config.js", "config.js", "config.ts",
    "database.yml", "database.yaml",
    "config/database.yml", "config/database.yaml",
    "config/settings.py", "settings.py",
    "appsettings.json", "appsettings.Development.json", "appsettings.Production.json",
    "application.properties", "application.yml", "application.yaml",
    "application-dev.properties", "application-prod.properties",
    "application-dev.yml", "application-dev.yaml",
]

# ─── Funciones auxiliares ─────────────────────────────────────────────────────

def read_file_safe(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""


def find_files(root: str, patterns: list) -> list:
    found = []
    for pattern in patterns:
        if "*" in pattern:
            found += glob.glob(os.path.join(root, "**", pattern), recursive=True)
        else:
            # Buscar tanto en la raíz como en subdirectorios
            found += glob.glob(os.path.join(root, "**", pattern), recursive=True)
            found += glob.glob(os.path.join(root, pattern))
    return list(set(found))


def parse_url_credentials(url: str, db_type: str, extra: dict = None) -> Optional[dict]:
    patterns = DB_URL_PATTERNS.get(db_type, [])
    for idx, pat in enumerate(patterns):
        m = re.search(pat, url, re.IGNORECASE)
        if m:
            groups = m.groups()
            if db_type == "sqlite":
                return {"type": db_type, "path": groups[0].strip(), "raw_url": url}
            elif db_type == "mysql" and idx == 1:
                # JDBC format: groups = (host, port, database)  — no user/pass in URL
                host = groups[0] or "localhost"
                port = int(groups[1]) if groups[1] else 3306
                database = (groups[2] or "").strip().split("?")[0]
                return {
                    "type": db_type,
                    "user": (extra or {}).get("user", ""),
                    "password": (extra or {}).get("password", ""),
                    "host": host,
                    "port": port,
                    "database": database,
                    "raw_url": url,
                }
            elif db_type in ("postgresql", "mysql"):
                return {
                    "type": db_type,
                    "user": groups[0] or "",
                    "password": groups[1] or "",
                    "host": groups[2] or "localhost",
                    "port": int(groups[3]) if groups[3] else (5432 if db_type == "postgresql" else 3306),
                    "database": (groups[4] or "").strip().split("?")[0],
                    "raw_url": url,
                }
            elif db_type == "mongodb":
                return {
                    "type": db_type,
                    "user": groups[0] or "",
                    "password": groups[1] or "",
                    "host": groups[2] or "localhost",
                    "port": int(groups[3]) if groups[3] else 27017,
                    "database": (groups[4] or "").strip().split("?")[0],
                    "raw_url": url,
                }
            elif db_type == "redis":
                return {
                    "type": db_type,
                    "user": groups[0] if len(groups) > 0 and groups[0] else "",
                    "password": groups[1] if len(groups) > 1 and groups[1] else "",
                    "host": groups[2] if len(groups) > 2 and groups[2] else "localhost",
                    "port": int(groups[3]) if len(groups) > 3 and groups[3] else 6379,
                    "database": groups[4] if len(groups) > 4 and groups[4] else "0",
                    "raw_url": url,
                }
    return None


def extract_individual_env_vars(content: str, db_type: str) -> Optional[dict]:
    """Extrae credenciales individuales como DB_HOST, DB_USER, etc."""
    def get(keys):
        for key in keys:
            m = re.search(rf'^{key}\s*=\s*["\']?([^"\'\n]+)["\']?', content, re.MULTILINE | re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    host_keys = ["DB_HOST", "POSTGRES_HOST", "MYSQL_HOST", "MONGO_HOST", "REDIS_HOST",
                 "DATABASE_HOST", "PG_HOST"]
    user_keys = ["DB_USER", "DB_USERNAME", "POSTGRES_USER", "MYSQL_USER", "MONGO_USER",
                 "DATABASE_USER", "DATABASE_USERNAME", "PG_USER"]
    pass_keys = ["DB_PASSWORD", "DB_PASS", "POSTGRES_PASSWORD", "MYSQL_PASSWORD",
                 "MONGO_PASSWORD", "DATABASE_PASSWORD", "DATABASE_PASS", "PG_PASSWORD"]
    name_keys = ["DB_NAME", "DB_DATABASE", "POSTGRES_DB", "MYSQL_DATABASE",
                 "MONGO_DATABASE", "DATABASE_NAME", "PG_DATABASE"]
    port_keys = ["DB_PORT", "POSTGRES_PORT", "MYSQL_PORT", "MONGO_PORT",
                 "REDIS_PORT", "DATABASE_PORT"]

    host = get(host_keys)
    user = get(user_keys)
    password = get(pass_keys)
    database = get(name_keys)
    port_str = get(port_keys)

    if host or user or database:
        default_ports = {"postgresql": 5432, "mysql": 3306, "mongodb": 27017,
                         "redis": 6379, "sqlite": None}
        return {
            "type": db_type,
            "host": host or "localhost",
            "user": user or "",
            "password": password or "",
            "database": database or "",
            "port": int(port_str) if port_str and port_str.isdigit() else default_ports.get(db_type),
        }
    return None


def extract_spring_datasource(content: str) -> Optional[dict]:
    """Extrae credenciales del formato Spring Boot (application.yaml / .properties)."""
    # YAML: datasource.url / datasource.username / datasource.password
    url_m  = re.search(r'url\s*:\s*["\']?(jdbc:[^\s"\']+)', content, re.IGNORECASE)
    user_m = re.search(r'username\s*:\s*["\']?([^\s"\'#\n]+)', content, re.IGNORECASE)
    pass_m = re.search(r'password\s*:\s*["\']?([^\s"\'#\n]*)', content, re.IGNORECASE)

    # .properties: spring.datasource.url=jdbc:...
    if not url_m:
        url_m  = re.search(r'datasource\.url\s*=\s*(jdbc:[^\s]+)', content, re.IGNORECASE)
        user_m = re.search(r'datasource\.username\s*=\s*([^\s\n]+)', content, re.IGNORECASE)
        pass_m = re.search(r'datasource\.password\s*=\s*([^\s\n]*)', content, re.IGNORECASE)

    if not url_m:
        return None

    jdbc_url = url_m.group(1).strip()
    extra = {
        "user":     user_m.group(1).strip() if user_m else "",
        "password": pass_m.group(1).strip() if pass_m else "",
    }

    # Detectar tipo desde jdbc:<type>://
    jdbc_type_m = re.match(r'jdbc:(\w+)', jdbc_url, re.IGNORECASE)
    if not jdbc_type_m:
        return None
    jdbc_type = jdbc_type_m.group(1).lower()

    db_type_map = {
        "mysql": "mysql", "mariadb": "mysql",
        "postgresql": "postgresql", "postgres": "postgresql",
        "sqlite": "sqlite", "sqlserver": "mssql", "oracle": "oracle",
    }
    db_type = db_type_map.get(jdbc_type)
    if not db_type or db_type not in DB_URL_PATTERNS:
        return None

    # Intentar extraer credenciales del URL embebidas (postgres://user:pass@host/db)
    creds = parse_url_credentials(jdbc_url, db_type, extra=extra)
    if creds:
        return creds

    # JDBC sin credenciales en la URL (jdbc:postgresql://host:port/db) — usar extra
    m = re.search(r'jdbc:\w+://([^:/\s]+)(?::(\d+))?/([^\s?\"\']+)', jdbc_url)
    if m:
        default_ports = {"postgresql": 5432, "mysql": 3306, "sqlite": None}
        return {
            "type":     db_type,
            "host":     m.group(1) or "localhost",
            "port":     int(m.group(2)) if m.group(2) else default_ports.get(db_type),
            "database": m.group(3).split("?")[0],
            "user":     extra.get("user", ""),
            "password": extra.get("password", ""),
            "raw_url":  jdbc_url,
        }
    return None


# ─── Detector principal ───────────────────────────────────────────────────────

class DatabaseDetector:
    def __init__(self, project_path: str):
        self.root = os.path.abspath(project_path)
        self.evidence = []   # lista de hallazgos
        self.scores = {db: 0 for db in DB_URL_PATTERNS}
        self.credentials = {}

    def _add_evidence(self, db_type: str, source: str, detail: str, weight: int = 1):
        self.scores[db_type] = self.scores.get(db_type, 0) + weight
        self.evidence.append({"db": db_type, "source": source, "detail": detail, "weight": weight})

    def scan_env_files(self):
        files = find_files(self.root, ENV_FILES)
        for fpath in files:
            content = read_file_safe(fpath)
            fname = os.path.relpath(fpath, self.root)

            # Spring Boot (primero — tiene user+pass completos)
            if any(x in fname for x in ["application.yaml", "application.yml",
                                         "application.properties"]):
                spring_creds = extract_spring_datasource(content)
                if spring_creds:
                    self.credentials[spring_creds["type"]] = spring_creds
                    self._add_evidence(spring_creds["type"], fname,
                                       "Credenciales Spring Boot datasource", weight=6)

            # Buscar URLs de conexión
            for db_type, patterns in DB_URL_PATTERNS.items():
                for pat in patterns:
                    matches = re.findall(pat, content, re.IGNORECASE)
                    if matches:
                        full_url_match = re.search(pat, content, re.IGNORECASE)
                        if full_url_match:
                            raw_url = full_url_match.group(0)
                            creds = parse_url_credentials(raw_url, db_type)
                            # Solo sobreescribir si no hay creds ya (o si las nuevas tienen más info)
                            if creds and (db_type not in self.credentials or
                                          (not self.credentials[db_type].get("user") and creds.get("user"))):
                                self.credentials[db_type] = creds
                        self._add_evidence(db_type, fname, f"URL de conexión encontrada", weight=5)

            # Buscar claves de entorno por nombre
            for db_type, keys in ENV_KEY_PATTERNS.items():
                for key in keys:
                    if re.search(rf'\b{key}\b', content, re.IGNORECASE):
                        self._add_evidence(db_type, fname, f"Variable '{key}' encontrada", weight=2)

            # Intentar extraer credenciales individuales
            for db_type in DB_URL_PATTERNS:
                if db_type not in self.credentials:
                    creds = extract_individual_env_vars(content, db_type)
                    if creds:
                        self.credentials[db_type] = creds

            # Parseo específico de configs cloud
            self._scan_cloud_config(content, fname)

            # Docker compose: imágenes
            if "docker-compose" in fname:
                for db_type, images in COMPOSE_IMAGE_PATTERNS.items():
                    for img in images:
                        if re.search(rf'image:\s*{img}', content, re.IGNORECASE):
                            self._add_evidence(db_type, fname, f"Imagen Docker '{img}'", weight=4)

    def _scan_cloud_config(self, content: str, fname: str):
        """Parsea archivos de configuración de plataformas cloud."""
        # render.yaml / fly.toml / railway — busca claves de BD conocidas
        if any(x in fname for x in ["render.yaml", "render.yml", "fly.toml", "railway"]):
            cloud_db_keys = [
                "DATABASE_URL", "DB_HOST", "DB_URL", "POSTGRES_URL",
                "MYSQL_URL", "MONGO_URL", "MONGODB_URI", "REDIS_URL",
                "DB_USERNAME", "DB_PASSWORD", "DB_NAME",
            ]
            for key in cloud_db_keys:
                if key in content:
                    # Detectar si el valor está hardcodeado (no sync: false)
                    # Ejemplo: key: DATABASE_URL\n  value: postgresql://...
                    m = re.search(
                        rf'{key}["\s:]+(?:value[:\s]+)?["\']?([^\s\'"{{}}]+)["\']?',
                        content, re.IGNORECASE
                    )
                    if m:
                        val = m.group(1).strip()
                        # Solo procesar si parece una URL o un host real (no placeholder)
                        if "://" in val:
                            for db_type, patterns in DB_URL_PATTERNS.items():
                                for pat in patterns:
                                    if re.match(pat, val, re.IGNORECASE):
                                        creds = parse_url_credentials(val, db_type)
                                        if creds and db_type not in self.credentials:
                                            self.credentials[db_type] = creds
                                        self._add_evidence(db_type, fname, f"URL cloud en {fname}", weight=6)

        # app.json (Heroku) — busca addons de BD
        if "app.json" in fname:
            heroku_addons = {
                "heroku-postgresql": "postgresql",
                "jawsdb": "mysql", "cleardb": "mysql",
                "mongolab": "mongodb", "mongohq": "mongodb",
                "rediscloud": "redis", "redistogo": "redis",
            }
            for addon, db_type in heroku_addons.items():
                if addon in content.lower():
                    self._add_evidence(db_type, fname, f"Addon Heroku '{addon}' detectado", weight=4)

        # .supabase/config.toml
        if "supabase" in fname:
            if "db_url" in content.lower() or "[db]" in content.lower():
                self._add_evidence("postgresql", fname, "Proyecto Supabase detectado", weight=5)

    def scan_dependency_files(self):
        files = find_files(self.root, DEP_FILES)
        for fpath in files:
            content = read_file_safe(fpath).lower()
            fname = os.path.relpath(fpath, self.root)
            for db_type, drivers in DRIVER_PATTERNS.items():
                for driver in drivers:
                    if driver.lower() in content:
                        self._add_evidence(db_type, fname, f"Driver '{driver}' detectado", weight=3)

    # Patrones de constructores ORM con parámetros separados (no URL)
    # Captura: (database, user, password, ...{ host, dialect/client })
    _ORM_CONSTRUCTOR_PATTERNS = [
        # Sequelize: new Sequelize('db', 'user', 'pass', { host: 'h', dialect: 'postgres'|'mysql' })
        (r"""new\s+Sequelize\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]*)['"]\s*,\s*\{[^}]*host\s*:\s*['"]([^'"]+)['"][^}]*dialect\s*:\s*['"](\w+)['"]""",
         "sequelize"),
        (r"""new\s+Sequelize\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]*)['"]\s*,\s*\{[^}]*dialect\s*:\s*['"](\w+)['"][^}]*host\s*:\s*['"]([^'"]+)['"]""",
         "sequelize_alt"),
        # knex({ client: 'pg'|'mysql', connection: { host, user, password, database } })
        (r"""client\s*:\s*['"](?:pg|postgres(?:ql)?)['"'][^}]*connection\s*:\s*\{[^}]*host\s*:\s*['"]([^'"]+)['"][^}]*user\s*:\s*['"]([^'"]+)['"][^}]*password\s*:\s*['"]([^'"]*)['"'][^}]*database\s*:\s*['"]([^'"]+)['"]""",
         "knex_pg"),
        (r"""client\s*:\s*['"](?:mysql2?)['"'][^}]*connection\s*:\s*\{[^}]*host\s*:\s*['"]([^'"]+)['"][^}]*user\s*:\s*['"]([^'"]+)['"][^}]*password\s*:\s*['"]([^'"]*)['"'][^}]*database\s*:\s*['"]([^'"]+)['"]""",
         "knex_mysql"),
        # Python psycopg2.connect(host=, user=, password=, dbname=)
        (r"""psycopg2\.connect\([^)]*host\s*=\s*['"]([^'"]+)['"][^)]*user\s*=\s*['"]([^'"]+)['"][^)]*password\s*=\s*['"]([^'"]*)['"'][^)]*dbname\s*=\s*['"]([^'"]+)['"]""",
         "psycopg2"),
        # Python psycopg2.connect con dbname primero
        (r"""psycopg2\.connect\([^)]*dbname\s*=\s*['"]([^'"]+)['"][^)]*host\s*=\s*['"]([^'"]+)['"][^)]*user\s*=\s*['"]([^'"]+)['"][^)]*password\s*=\s*['"]([^'"]*)['"']""",
         "psycopg2_alt"),
        # pymysql.connect(host=, user=, password=, database=)
        (r"""pymysql\.connect\([^)]*host\s*=\s*['"]([^'"]+)['"][^)]*user\s*=\s*['"]([^'"]+)['"][^)]*password\s*=\s*['"]([^'"]*)['"'][^)]*(?:database|db)\s*=\s*['"]([^'"]+)['"]""",
         "pymysql"),
    ]

    # Patrones de connect() directo para SQLite en varios lenguajes
    _SQLITE_CONNECT_PATTERNS = [
        # Python: sqlite3.connect("file.db") / sqlite3.connect('file.db')
        r'sqlite3\.connect\(["\']([^"\']+\.(?:db|sqlite|sqlite3))["\']',
        # Python: create_engine("sqlite:///file.db") — ya cubierto por DB_URL_PATTERNS
        # JS/TS: new Database("file.db") / better-sqlite3
        r'new\s+Database\(["\']([^"\']+\.(?:db|sqlite|sqlite3))["\']',
        # JS/TS: knex({ client: 'sqlite3', connection: { filename: "file.db" } })
        r'filename["\']?\s*:\s*["\']([^"\']+\.(?:db|sqlite|sqlite3))["\']',
        # Ruby: SQLite3::Database.new("file.db")
        r'SQLite3::Database\.new\(["\']([^"\']+\.(?:db|sqlite|sqlite3))["\']',
        # Java/Spring: jdbc:sqlite:file.db
        r'jdbc:sqlite:([^\s"\']+)',
    ]

    def scan_source_files(self):
        """Escanea archivos de código fuente buscando strings de conexión."""
        extensions = ["*.py", "*.js", "*.ts", "*.rb", "*.go", "*.java",
                      "*.php", "*.cs", "*.rs", "*.env*", "*.config*", "*.yaml", "*.yml"]
        for ext in extensions:
            for fpath in glob.glob(os.path.join(self.root, "**", ext), recursive=True):
                # Saltar node_modules, .git, venv, etc.
                rel = os.path.relpath(fpath, self.root)
                if any(skip in rel for skip in ["node_modules", ".git", "venv",
                                                 "__pycache__", "dist", "build"]):
                    continue
                content = read_file_safe(fpath)
                for db_type, patterns in DB_URL_PATTERNS.items():
                    for pat in patterns:
                        m = re.search(pat, content, re.IGNORECASE)
                        if m:
                            raw_url = m.group(0)
                            creds = parse_url_credentials(raw_url, db_type)
                            if creds and db_type not in self.credentials:
                                self.credentials[db_type] = creds
                            self._add_evidence(db_type, rel, "String de conexión en código", weight=4)

                # Patrones específicos de sqlite3.connect() y equivalentes
                if "sqlite" not in self.credentials:
                    for pat in self._SQLITE_CONNECT_PATTERNS:
                        m = re.search(pat, content, re.IGNORECASE)
                        if m:
                            db_path = m.group(1).strip()
                            # Convertir ruta relativa a absoluta desde la raíz del proyecto
                            if not os.path.isabs(db_path):
                                db_path = os.path.join(self.root, db_path)
                            self.credentials["sqlite"] = {"type": "sqlite", "path": db_path}
                            self._add_evidence("sqlite", rel, "sqlite3.connect() en código", weight=5)
                            break

                # Constructores ORM con parámetros separados (Sequelize, knex, psycopg2, pymysql)
                _dialect_map = {
                    "postgres": "postgresql", "postgresql": "postgresql",
                    "mysql": "mysql", "mysql2": "mysql",
                    "sqlite": "sqlite", "sqlite3": "sqlite",
                    "mongodb": "mongodb", "mongo": "mongodb",
                }
                for pat, kind in self._ORM_CONSTRUCTOR_PATTERNS:
                    m = re.search(pat, content, re.IGNORECASE | re.DOTALL)
                    if not m:
                        continue
                    g = m.groups()
                    try:
                        if kind == "sequelize":
                            # groups: (database, user, password, host, dialect)
                            db_type = _dialect_map.get(g[4].lower(), "postgresql")
                            creds = {"type": db_type, "database": g[0], "user": g[1],
                                     "password": g[2], "host": g[3],
                                     "port": 5432 if db_type == "postgresql" else 3306}
                        elif kind == "sequelize_alt":
                            # groups: (database, user, password, dialect, host)
                            db_type = _dialect_map.get(g[3].lower(), "postgresql")
                            creds = {"type": db_type, "database": g[0], "user": g[1],
                                     "password": g[2], "host": g[4],
                                     "port": 5432 if db_type == "postgresql" else 3306}
                        elif kind in ("knex_pg", "psycopg2"):
                            # groups: (host, user, password, database)
                            db_type = "postgresql"
                            creds = {"type": db_type, "host": g[0], "user": g[1],
                                     "password": g[2], "database": g[3], "port": 5432}
                        elif kind == "psycopg2_alt":
                            # groups: (database, host, user, password)
                            db_type = "postgresql"
                            creds = {"type": db_type, "database": g[0], "host": g[1],
                                     "user": g[2], "password": g[3], "port": 5432}
                        elif kind in ("knex_mysql", "pymysql"):
                            # groups: (host, user, password, database)
                            db_type = "mysql"
                            creds = {"type": db_type, "host": g[0], "user": g[1],
                                     "password": g[2], "database": g[3], "port": 3306}
                        else:
                            continue
                        if db_type not in self.credentials:
                            self.credentials[db_type] = creds
                            self._add_evidence(db_type, rel,
                                               f"Constructor ORM ({kind}) en código", weight=5)
                    except (IndexError, TypeError):
                        continue

    def _fallback_sqlite_file(self):
        """Busca archivos .db/.sqlite en la raíz del proyecto como último recurso."""
        for ext in ("*.db", "*.sqlite", "*.sqlite3"):
            matches = glob.glob(os.path.join(self.root, ext))
            if matches:
                # Preferir el más grande (más probable que sea la BD principal)
                matches.sort(key=lambda f: os.path.getsize(f), reverse=True)
                return matches[0]
        return None

    def detect(self) -> dict:
        if not os.path.isdir(self.root):
            return {"error": f"Carpeta no encontrada: {self.root}"}

        self.scan_env_files()
        self.scan_dependency_files()
        self.scan_source_files()

        # Fallback SQLite: buscar archivos .db directamente si el score indica SQLite
        # pero no se extrajeron credenciales, o si no se detectó nada pero hay un .db
        if "sqlite" not in self.credentials:
            db_file = self._fallback_sqlite_file()
            if db_file:
                self.credentials["sqlite"] = {"type": "sqlite", "path": db_file}
                self.scores["sqlite"] = max(self.scores.get("sqlite", 0), 3)
                self._add_evidence("sqlite", os.path.basename(db_file), "Archivo de base de datos encontrado", weight=3)

        # Ordenar por puntuación
        ranked = sorted(
            [(db, score) for db, score in self.scores.items() if score > 0],
            key=lambda x: x[1], reverse=True
        )

        primary = ranked[0][0] if ranked else None

        # Marcar si las credenciales son cloud (host no es localhost/127.0.0.1)
        local_hosts = {"localhost", "127.0.0.1", "0.0.0.0", "", None}
        for creds in self.credentials.values():
            host = creds.get("host", "")
            creds["is_cloud"] = host not in local_hosts

        # Solo conservar credenciales para tipos con evidencia real
        detected_types = {db for db, _ in ranked}
        filtered_creds = {k: v for k, v in self.credentials.items() if k in detected_types}

        # Eliminar credenciales duplicadas: si varios tipos comparten exactamente
        # el mismo user+host+database, es señal de que provienen de keys genéricos
        # (DB_HOST, DB_USER…). En ese caso, quedarse solo con el tipo primario.
        if primary and primary in filtered_creds:
            primary_creds = filtered_creds[primary]
            primary_sig = (primary_creds.get("user"), primary_creds.get("host"),
                           primary_creds.get("database"))
            filtered_creds = {
                k: v for k, v in filtered_creds.items()
                if k == primary
                or (v.get("user"), v.get("host"), v.get("database")) != primary_sig
            }

        # Buscar docker-compose en la raíz del proyecto o hasta 2 niveles arriba
        compose_file = None
        compose_names = ("docker-compose.yml", "docker-compose.yaml",
                         "docker-compose.override.yml", "compose.yml", "compose.yaml")
        search_dirs = [self.root, os.path.dirname(self.root), os.path.dirname(os.path.dirname(self.root))]
        for search_dir in search_dirs:
            for name in compose_names:
                candidate = os.path.join(search_dir, name)
                if os.path.isfile(candidate):
                    compose_file = candidate
                    break
            if compose_file:
                break

        app_start = self._detect_app_start()

        return {
            "project_path": self.root,
            "primary_db": primary,
            "all_detected": [{"type": db, "score": score} for db, score in ranked],
            "credentials": filtered_creds,
            "evidence": self.evidence,
            "compose_file": compose_file,
            "app_start": app_start,
        }

    def _detect_app_start(self) -> Optional[dict]:
        """Detect how to start the project application."""
        import shutil
        root = Path(self.root)

        # ── Spring Boot (Maven) ───────────────────────────────────────────────
        if (root / "mvnw").exists():
            return {"type": "springboot", "label": "Spring Boot (Maven Wrapper)",
                    "cmd": "./mvnw spring-boot:run", "cwd": str(root)}
        if (root / "pom.xml").exists() and shutil.which("mvn"):
            return {"type": "springboot", "label": "Spring Boot (Maven)",
                    "cmd": "mvn spring-boot:run", "cwd": str(root)}

        # ── Spring Boot (Gradle) ──────────────────────────────────────────────
        if (root / "gradlew").exists():
            return {"type": "springboot", "label": "Spring Boot (Gradle Wrapper)",
                    "cmd": "./gradlew bootRun", "cwd": str(root)}
        has_gradle_build = (root / "build.gradle").exists() or (root / "build.gradle.kts").exists()
        if has_gradle_build and shutil.which("gradle"):
            return {"type": "springboot", "label": "Spring Boot (Gradle)",
                    "cmd": "gradle bootRun", "cwd": str(root)}
        # Gradle build exists but no wrapper and no gradle binary → sugerir wrapper
        if has_gradle_build:
            return {"type": "springboot_no_wrapper", "label": "Spring Boot (Gradle)",
                    "cmd": None, "cwd": str(root),
                    "missing": "gradle",
                    "fix": "Instala Gradle o genera el wrapper con: gradle wrapper"}

        # ── Node.js ───────────────────────────────────────────────────────────
        pkg = root / "package.json"
        if pkg.exists():
            try:
                pkg_data = json.loads(pkg.read_text(errors="replace"))
                scripts  = pkg_data.get("scripts", {})
                mgr = "npm"
                if (root / "yarn.lock").exists():    mgr = "yarn"
                elif (root / "pnpm-lock.yaml").exists(): mgr = "pnpm"

                # Preferir npm local, caer en sistema
                if _IS_WINDOWS:
                    _local_npm = os.path.join(os.environ.get("LOCALAPPDATA", ""), "node", "npm.cmd")
                else:
                    _local_npm = os.path.expanduser("~/.local/share/node/bin/npm")
                if os.path.isfile(_local_npm):
                    npm_exe = f'"{_local_npm}"'
                    has_npm = True
                else:
                    has_npm = bool(shutil.which("npm" if mgr == "npm" else mgr))
                    npm_exe = mgr

                run = "dev" if "dev" in scripts else ("start" if "start" in scripts else None)
                if run:
                    if has_npm:
                        return {"type": "nodejs", "label": f"Node.js ({mgr} run {run})",
                                "cmd": f"{npm_exe} run {run}", "cwd": str(root),
                                "run_script": run, "mgr": mgr}
                    return {"type": "nodejs", "label": f"Node.js ({mgr} run {run})",
                            "cmd": None, "cwd": str(root),
                            "missing": "node", "run_script": run, "mgr": mgr,
                            "fix": "Instala Node.js para poder ejecutar este proyecto"}

                # Sin scripts: usar "main" del package.json o buscar entry point común
                main = pkg_data.get("main")
                if not main:
                    for candidate in ("app.js", "index.js", "server.js", "main.js", "src/index.js", "src/app.js"):
                        if (root / candidate).exists():
                            main = candidate
                            break
                if main:
                    if _IS_WINDOWS:
                        _local_node = os.path.join(os.environ.get("LOCALAPPDATA", ""), "node", "node.exe")
                    else:
                        _local_node = os.path.expanduser("~/.local/share/node/bin/node")
                    if os.path.isfile(_local_node):
                        return {"type": "nodejs", "label": f"Node.js (node {main})",
                                "cmd": f'"{_local_node}" {main}', "cwd": str(root)}
                    if shutil.which("node"):
                        return {"type": "nodejs", "label": f"Node.js (node {main})",
                                "cmd": f"node {main}", "cwd": str(root)}
                    return {"type": "nodejs", "label": f"Node.js (node {main})",
                            "cmd": None, "cwd": str(root),
                            "missing": "node",
                            "fix": "Instala Node.js para poder ejecutar este proyecto"}
            except Exception:
                pass

        # ── Django ────────────────────────────────────────────────────────────
        if (root / "manage.py").exists():
            py = "python3" if os.path.exists("/usr/bin/python3") else "python"
            return {"type": "django", "label": "Django",
                    "cmd": f"{py} manage.py runserver", "cwd": str(root)}

        # ── Flask / FastAPI ───────────────────────────────────────────────────
        for entry in ["app.py", "main.py", "run.py", "wsgi.py", "asgi.py"]:
            if (root / entry).exists():
                py = "python3" if os.path.exists("/usr/bin/python3") else "python"
                return {"type": "python", "label": f"Python ({entry})",
                        "cmd": f"{py} {entry}", "cwd": str(root)}

        # ── Ruby on Rails ─────────────────────────────────────────────────────
        if (root / "Gemfile").exists():
            return {"type": "rails", "label": "Ruby on Rails",
                    "cmd": "bundle exec rails server", "cwd": str(root)}

        # ── Laravel / PHP ─────────────────────────────────────────────────────
        if (root / "artisan").exists():
            return {"type": "laravel", "label": "Laravel",
                    "cmd": "php artisan serve", "cwd": str(root)}

        # ── Go ────────────────────────────────────────────────────────────────
        if (root / "main.go").exists():
            return {"type": "go", "label": "Go",
                    "cmd": "go run .", "cwd": str(root)}

        return None

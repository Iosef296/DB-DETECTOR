import hashlib
import os
import json
import shutil
import signal
import socket
import struct
import subprocess
import platform
import time
from pathlib import Path

from core.paths import data_root

_IS_WINDOWS = platform.system() == "Windows"

# Archivo JSON que NativeManager escribe en la carpeta del proyecto con la config de BD
_NATIVE_CONFIG_FILE = ".orq_native.json"


def _data_base() -> str:
    # Directorio donde se guardan los datos de BD (pgdata, datos de MySQL, etc.).
    # Se resuelve en cada llamada (no a nivel de módulo) para respetar ORQ_DATA_DIR
    # aunque se setee después de importar.
    path = os.path.join(data_root(), "native")
    os.makedirs(path, exist_ok=True)
    return path

# Puerto estándar de cada BD dentro del proceso nativo
_CONTAINER_PORTS = {
    "postgresql": 5432,
    "mysql":      3306,
    "mariadb":    3306,
    "mongodb":    27017,
    "redis":      6379,
}

# Variables de entorno que cada BD usa para user/password/database en docker-compose
# (reutilizado para parsear compose.yml sin depender de código de DockerManager)
_DB_ENV_FIELDS = {
    "postgresql": {"user": ["POSTGRES_USER"], "password": ["POSTGRES_PASSWORD"], "database": ["POSTGRES_DB"]},
    "mysql":      {"user": ["MYSQL_USER", "MARIADB_USER"], "password": ["MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"], "database": ["MYSQL_DATABASE", "MARIADB_DATABASE"]},
    "mariadb":    {"user": ["MARIADB_USER", "MYSQL_USER"], "password": ["MARIADB_PASSWORD", "MARIADB_ROOT_PASSWORD"], "database": ["MARIADB_DATABASE", "MYSQL_DATABASE"]},
    "mongodb":    {"user": ["MONGO_INITDB_ROOT_USERNAME"], "password": ["MONGO_INITDB_ROOT_PASSWORD"], "database": ["MONGO_INITDB_DATABASE"]},
    "redis":      {"password": ["REDIS_PASSWORD", "REQUIREPASS"]},
}


def _load_dotenv(compose_file: str) -> dict:
    """Carga el .env que docker-compose busca automáticamente junto al compose.yml
    (mismo directorio), para poder resolver placeholders ${VAR} del compose."""
    env_path = os.path.join(os.path.dirname(compose_file), ".env")
    values: dict = {}
    try:
        with open(env_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                values[key.strip()] = val.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return values


def _resolve_compose_placeholder(raw: str, dotenv: dict) -> str | None:
    """Si `raw` es un placeholder tipo ${VAR} o ${VAR:-default} (docker-compose no
    resuelve esta interpolación por sí mismo: el propio motor de compose la resuelve
    leyendo el .env, y como nosotros solo usamos regex sobre el YAML en lugar del CLI
    de docker-compose, hay que resolverla a mano), lo resuelve contra el .env del
    proyecto y si no, contra el entorno del proceso. Devuelve None si no se puede
    resolver, para no propagar un placeholder roto (ej. literal "${DB_USER}",
    que Postgres rechaza por el carácter '$' al crear la BD)."""
    import re
    m = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-(.*))?\}", raw)
    if not m:
        return raw  # valor literal normal, no es un placeholder
    var_name, _, default = m.groups()
    if var_name in dotenv:
        return dotenv[var_name]
    if var_name in os.environ:
        return os.environ[var_name]
    return default  # puede ser None si tampoco había default


def _parse_compose_to_config(compose_file: str, db_type: str) -> dict:
    """Extrae config de BD de un docker-compose.yml usando regex (sin parser YAML)."""
    import re
    try:
        with open(compose_file, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception:
        return {}

    dotenv = _load_dotenv(compose_file)
    container_port = _CONTAINER_PORTS.get(db_type)
    config: dict = {"db_type": db_type, "credentials": {}}

    if container_port:
        # Extraer el puerto host del mapeo "host:contenedor"
        m = re.search(r'["\']?(\d+):' + str(container_port) + r'["\']?', content)
        if m:
            config["port"] = int(m.group(1))

    # Extraer credenciales de las variables de entorno
    env_map = _DB_ENV_FIELDS.get(db_type, {})
    for field, env_vars in env_map.items():
        for var in env_vars:
            m = re.search(rf'{var}\s*[=:]\s*["\']?([^"\'\s\n]+)["\']?', content)
            if m:
                resolved = _resolve_compose_placeholder(m.group(1), dotenv)
                if resolved:
                    config["credentials"][field] = resolved
                break

    return config


class _PgAuthError(Exception):
    """Conexión TCP ok pero el servidor rechazó user/password (contraseña desincronizada)."""


def _pg_recv_exact(s, n):
    # TCP puede entregar los datos en fragmentos más pequeños que n bytes.
    # recv(n) no garantiza recibir exactamente n bytes, solo "hasta n".
    # Acumulamos en un buffer hasta tener exactamente lo que necesitamos.
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf


def _pg_read_msg(s):
    # Formato de mensaje del protocolo PostgreSQL 3.0:
    #   1 byte  → tipo de mensaje ('R'=Auth, 'Z'=ReadyForQuery, 'E'=Error, etc.)
    #   4 bytes → longitud total del mensaje (incluyendo estos 4 bytes, pero no el tipo)
    #   N bytes → body (longitud - 4 bytes)
    mt = s.recv(1)
    if not mt:
        raise ConnectionError("closed")
    length = struct.unpack("!I", _pg_recv_exact(s, 4))[0]
    body = _pg_recv_exact(s, length - 4) if length > 4 else b""
    return mt, body


def _pg_drain_to_ready(s):
    # Después de enviar una query, el servidor manda varios mensajes antes de Z (ReadyForQuery).
    # Tenemos que consumirlos todos o el próximo read leerá mensajes del comando anterior.
    while True:
        mt, _ = _pg_read_msg(s)
        if mt == b'Z':
            return True
        if mt == b'E':
            return False


def _pg_connect(host: str, port: int, user: str, password: str, database: str = "postgres", timeout: int = 10):
    """
    Conecta y autentica contra PostgreSQL usando el wire protocol directamente sobre
    un socket TCP, sin psycopg2 ni binarios externos (createdb, psql):
      - psycopg2 es una dependencia C que el usuario puede no tener instalada.
      - los binarios externos pueden no estar en PATH (ej. instalación mínima).
      - el wire protocol de PostgreSQL 3.0 es estable desde PostgreSQL 7.4 y no cambia.
    Devuelve el socket ya en estado ReadyForQuery. Lanza _PgAuthError si la conexión
    TCP funcionó pero el servidor rechazó la autenticación (útil para distinguir
    "contraseña desincronizada" de "postgres todavía no levantó").
    """
    s = socket.create_connection((host, port), timeout=timeout)
    try:
        # Formato: longitud(4) + versión(4=196608=3.0) + "user\0<user>\0database\0<database>\0\0"
        params = f"user\x00{user}\x00database\x00{database}\x00\x00".encode("utf-8")
        s.sendall(struct.pack("!II", 8 + len(params), 196608) + params)

        while True:
            mt, body = _pg_read_msg(s)
            if mt == b'R':
                auth_type = struct.unpack("!I", body[:4])[0]
                if auth_type == 0:
                    # AuthenticationOk → autenticado sin contraseña (trust auth)
                    continue
                elif auth_type == 5:
                    # MD5Password: el servidor envía un salt de 4 bytes.
                    # PostgreSQL usa: "md5" + md5(md5(password+user) + salt)
                    # El doble hash evita ataques de preimage: incluso si interceptan
                    # el hash enviado, no pueden reutilizarlo en otra sesión (el salt cambia).
                    salt = body[4:8]
                    inner = hashlib.md5((password + user).encode("utf-8")).hexdigest()
                    outer = hashlib.md5(inner.encode("ascii") + salt).hexdigest()
                    pw = f"md5{outer}\x00".encode("ascii")
                    s.sendall(b'p' + struct.pack("!I", 4 + len(pw)) + pw)
                else:
                    # SCRAM-SHA-256, Kerberos, etc.: no implementados.
                    # En instalaciones portables siempre usamos md5 (configurado en pg_hba.conf).
                    raise _PgAuthError("unsupported auth method")
            elif mt in (b'K', b'N', b'S'):
                # K=BackendKeyData (PID+secret key para cancelar queries)
                # N=NoticeResponse (advertencias no fatales)
                # S=ParameterStatus (encoding, TimeZone, etc.)
                # Ninguno requiere respuesta; los ignoramos para llegar a Z.
                continue
            elif mt == b'Z':
                return s  # ReadyForQuery → sesión establecida, podemos enviar queries
            elif mt == b'E':
                raise _PgAuthError(body.decode("utf-8", "replace"))
    except _PgAuthError:
        s.close()
        raise
    except Exception:
        s.close()
        raise


def _pg_create_database(host: str, port: int, user: str, password: str, database: str) -> bool:
    """Crea 'database' si no existe. False en cualquier error (conexión, auth o query)."""
    try:
        s = _pg_connect(host, port, user, password, database="postgres")
    except Exception:
        return False

    try:
        # Consultar pg_database en lugar de intentar CREATE DATABASE directamente,
        # porque CREATE DATABASE en una BD que existe lanza un error que rompe la sesión.
        q = f"SELECT 1 FROM pg_database WHERE datname='{database}'\x00".encode()
        s.sendall(b'Q' + struct.pack("!I", 4 + len(q)) + q)
        exists = False
        while True:
            mt, _ = _pg_read_msg(s)
            if mt == b'D':
                exists = True  # DataRow → al menos una fila → la BD existe
            elif mt in (b'Z', b'E'):
                break

        if not exists:
            q = f'CREATE DATABASE "{database}"\x00'.encode()
            s.sendall(b'Q' + struct.pack("!I", 4 + len(q)) + q)
            _pg_drain_to_ready(s)

        # Terminate (mensaje 'X') cierra la sesión de forma limpia desde el cliente.
        # Sin esto, el servidor marca la conexión como "abruptamente cerrada" en los logs,
        # lo que puede llenar el log file de mensajes de warning innecesarios.
        s.sendall(b'X' + struct.pack("!I", 4))
        return True
    except Exception:
        return False
    finally:
        try:
            s.close()
        except Exception:
            pass


# ── NativeManager ──────────────────────────────────────────────────────────────
# Alternativa a DockerManager cuando Docker no está disponible.
# Usa los binarios de BD instalados en el sistema operativo (pg_ctl, mysqld, mongod, redis-server).
# PortableManager extiende esta clase para auto-descargar los binarios si no están instalados.

class NativeManager:

    def is_docker_available(self) -> bool:
        # NativeManager nunca usa Docker
        return False

    # ── Resolución de binarios (sobreescrito en PortableManager) ─────────────

    def _bin(self, name: str, db_type: str = "") -> str | None:
        # Buscar el binario en el PATH del sistema
        return shutil.which(name)

    def _proc_env(self, db_type: str) -> dict:
        # Heredar el entorno del proceso actual (PortableManager añade LD_LIBRARY_PATH)
        return os.environ.copy()

    def create_network(self, project_name: str) -> bool:
        return True  # Sin Docker no hay redes; devolver True para compatibilidad

    def remove_network(self, project_name: str) -> bool:
        return True

    # ── Helpers de configuración ──────────────────────────────────────────────

    def _config_path(self, project_path: str) -> str:
        # El archivo .orq_native.json vive en la raíz del proyecto
        return os.path.join(project_path, _NATIVE_CONFIG_FILE)

    def _load_config(self, project_path: str) -> dict:
        try:
            with open(self._config_path(project_path), encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _data_dir(self, project_path: str, db_type: str) -> str:
        # Directorio donde se almacenan los datos de la BD (ej. ~/.orquestador/native/miapp/postgresql)
        project_name = Path(project_path).name
        path = os.path.join(_data_base(), project_name, db_type)
        os.makedirs(path, exist_ok=True)
        return path

    # ── Bajada de privilegios (contenedores que corren como root, ej. Railway) ─
    # PostgreSQL (initdb/postgres) se niega a correr como root. En un contenedor
    # que arranca como root hay que ejecutar esos procesos con un uid sin privilegios.

    def _unprivileged_ids(self) -> "tuple[int, int] | None":
        """(uid, gid) sin privilegios si el proceso actual es root; None si no aplica."""
        if not hasattr(os, "geteuid"):
            return None  # Windows
        try:
            if os.geteuid() != 0:
                return None  # ya somos no-root, nada que hacer
        except Exception:
            return None
        uid = int(os.environ.get("ORQ_UNPRIVILEGED_UID", "1000"))
        gid = int(os.environ.get("ORQ_UNPRIVILEGED_GID", str(uid)))
        return uid, gid

    def _su_kwargs(self, *chown_dirs: str) -> dict:
        """Devuelve kwargs {user,group} para subprocess y hace chown recursivo de
        los directorios dados para que el proceso no-root pueda escribir en ellos.
        Vacío si no hace falta bajar privilegios (no somos root, o Windows)."""
        ids = self._unprivileged_ids()
        if not ids:
            return {}
        uid, gid = ids
        for d in chown_dirs:
            if not d or not os.path.isdir(d):
                continue
            try:
                os.chown(d, uid, gid)
                for root_, dnames, fnames in os.walk(d):
                    for name in dnames + fnames:
                        try:
                            os.chown(os.path.join(root_, name), uid, gid)
                        except OSError:
                            pass
            except OSError:
                pass
        return {"user": uid, "group": gid}

    # ── Métodos de inicio de BD ───────────────────────────────────────────────

    def _pg_reset_password(self, pg_data: str, data_dir: str, port: int,
                            user: str, password: str, env: dict) -> bool:
        """
        Reconcilia el rol 'user' cuando pgdata ya estaba inicializado con otras
        credenciales (ej. se corrigieron después del initdb original, pero initdb
        solo corre una vez y nunca vuelve a tocar lo que quedó en disco). Cubre
        tanto "el rol existe con otro password" como "el rol ni siquiera existe"
        (initdb se corrió con -U distinto al usuario configurado ahora).
        Se usa el modo single-user de postgres (`postgres --single`), que lee SQL
        directo del data dir sin pasar por autenticación de red ni por el rol
        bootstrap real, así no hace falta saber cuál era ese rol original.
        """
        pg_ctl = self._bin("pg_ctl", "postgresql")
        postgres_bin = self._bin("postgres", "postgresql")
        if not pg_ctl or not postgres_bin or not os.path.exists(pg_data):
            return False

        su = self._su_kwargs(data_dir)
        log_file = os.path.join(data_dir, "postgresql.log")

        # El modo single-user no admite conexiones concurrentes: hay que parar
        # el servidor normal primero (si está corriendo) y volver a levantarlo después.
        subprocess.run([pg_ctl, "-D", pg_data, "stop", "-m", "fast"],
                        capture_output=True, encoding="utf-8", errors="replace", env=env, **su)

        safe_ident = user.replace('"', '""')
        safe_lit = user.replace("'", "''")
        safe_pw = password.replace("'", "''")
        sql = (
            "DO $$ BEGIN "
            f"IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{safe_lit}') THEN "
            f"CREATE ROLE \"{safe_ident}\" LOGIN SUPERUSER PASSWORD '{safe_pw}'; "
            "ELSE "
            f"ALTER ROLE \"{safe_ident}\" WITH LOGIN PASSWORD '{safe_pw}'; "
            "END IF; END $$;\n"
        )
        try:
            r = subprocess.run(
                [postgres_bin, "--single", "-D", pg_data, "postgres"],
                input=sql, capture_output=True, encoding="utf-8", errors="replace",
                env=env, timeout=30, **su
            )
            output = (r.stdout or "") + (r.stderr or "")
            ok = r.returncode == 0 and "ERROR" not in output
        except Exception:
            ok = False

        subprocess.run([pg_ctl, "-D", pg_data, "-l", log_file, "-o", f"-p {port}", "-w", "start"],
                        capture_output=True, encoding="utf-8", errors="replace", env=env, **su)
        # Esperar a que el puerto acepte conexiones antes de devolver el control:
        # el caller llama _pg_create_database justo después de este reconcile, y
        # sin esta espera esa llamada puede llegar antes de que postgres esté
        # listo, fallando por timing y no por auth (el rol ya quedó bien creado).
        for _ in range(30):
            try:
                with socket.create_connection(("localhost", port), timeout=1):
                    break
            except OSError:
                time.sleep(1)
        return ok

    def _pg_reconcile_and_create(self, pg_data: str, data_dir: str, port: int,
                                   user: str, password: str, database: str, env: dict,
                                   attempts: int = 3) -> bool:
        """
        Reintenta reconciliar rol + crear BD varias veces en vez de una sola.
        Un único intento alcanza en el caso normal (pgdata con credenciales
        viejas), pero bajo presión de RAM (ej. arrancando un grupo grande de
        servicios a la vez) el `postgres --single` del reconcile puede
        colgarse/fallar por falta de recursos momentánea, no por un problema
        real de auth. Reintentar con backoff le da tiempo al sistema a
        liberar memoria (el arranque de los otros servicios termina, o el
        propio OS libera standby list) antes de reportar error definitivo.
        """
        for intento in range(attempts):
            if self._pg_reset_password(pg_data, data_dir, port, user, password, env) and \
               _pg_create_database("localhost", port, user, password, database):
                return True
            if intento < attempts - 1:
                time.sleep(2 * (intento + 1))
        return False

    def _start_postgresql(self, data_dir: str, port: int,
                          user: str = "dbuser", password: str = "dbpass",
                          database: str = "postgres", **_) -> dict:
        pg_ctl  = self._bin("pg_ctl",  "postgresql")
        initdb  = self._bin("initdb",  "postgresql")
        if not pg_ctl or not initdb:
            return {"ok": False, "error": "PostgreSQL no disponible. Instala con: sudo apt install postgresql postgresql-client"}

        env      = self._proc_env("postgresql")
        pg_data  = os.path.join(data_dir, "pgdata")  # directorio de datos de PostgreSQL
        log_file = os.path.join(data_dir, "postgresql.log")
        # HOME explícito: initdb/postgres lo consultan y el uid sin privilegios
        # puede no tener entrada en /etc/passwd.
        env["HOME"] = data_dir

        # postmaster.pid existe si y solo si PostgreSQL arrancó correctamente.
        # Contiene: PID (línea 0), data dir (línea 1), hora de inicio (línea 2), puerto (línea 3).
        # Lo leemos para detectar si el proceso todavía está vivo ANTES de llamar pg_ctl start,
        # porque pg_ctl start fallaría con "lock file exists" si el proceso ya está corriendo.
        pid_file = os.path.join(pg_data, "postmaster.pid")
        if os.path.exists(pid_file):
            try:
                lines = open(pid_file).readlines()
                pid          = int(lines[0].strip())
                running_port = int(lines[3].strip()) if len(lines) > 3 else None
                # os.kill(pid, 0) NO envía ninguna señal; solo verifica que el proceso existe.
                # Si lanza ProcessLookupError → el proceso murió pero dejó el PID file (stale).
                # Si lanza PermissionError → el proceso existe pero es de otro usuario (igual sirve).
                os.kill(pid, 0)
                if running_port == port:
                    # Ya está vivo en el puerto correcto → nada que hacer salvo crear la BD si falta.
                    if database and database not in ("postgres", "template0", "template1"):
                        if not _pg_create_database("localhost", port, user, password, database):
                            # Falló (probablemente auth): pgdata ya estaba inicializado con un
                            # password distinto al guardado ahora. Reconciliamos y reintentamos
                            # en vez de reportar RUNNING con una contraseña desincronizada.
                            if self._pg_reconcile_and_create(pg_data, data_dir, port, user, password, database, env):
                                return {"ok": True}
                            return {"ok": False, "error": f"PostgreSQL corriendo pero la autenticación para '{user}' falló (contraseña desincronizada con el pgdata ya inicializado)."}
                    return {"ok": True}
                # Está vivo pero en un puerto distinto (ej. el usuario cambió db_port).
                # Paramos con "fast" (rollback inmediato de transacciones) en lugar de "smart"
                # (esperar a que todos los clientes se desconecten) para no bloquear indefinidamente.
                subprocess.run(
                    [pg_ctl, "-D", pg_data, "stop", "-m", "fast"],
                    capture_output=True, encoding="utf-8", errors="replace", env=env,
                    **self._su_kwargs(data_dir)
                )
            except (ProcessLookupError, PermissionError):
                pass  # PID stale → el proceso ya no existe, ignorar y arrancar
            except Exception:
                pass
            try:
                os.remove(pid_file)
            except Exception:
                pass

        # PG_VERSION es el archivo que initdb escribe al final de su ejecución exitosa.
        # Si existe, el directorio ya fue inicializado; volver a correr initdb corrompería los datos.
        # Si no existe (primera vez, o directorio borrado a mano), hay que inicializar.
        if not os.path.exists(os.path.join(pg_data, "PG_VERSION")):
            os.makedirs(pg_data, exist_ok=True)
            # La contraseña se pasa por archivo en lugar de por argumento de línea de comandos
            # porque los argumentos son visibles en "ps aux" para otros usuarios del sistema.
            # El archivo se borra inmediatamente después de que initdb lo lee.
            pwfile = os.path.join(data_dir, ".pgpass")
            with open(pwfile, "w") as f:
                f.write(password + "\n")
            # chown del árbol de datos ANTES de initdb (incluye pg_data y pwfile recién creados)
            su = self._su_kwargs(data_dir)
            r = subprocess.run(
                [initdb, "-D", pg_data, "-U", user or "postgres",
                 "--auth=md5", f"--pwfile={pwfile}"],
                capture_output=True, encoding="utf-8", errors="replace", env=env, **su
            )
            try:
                os.remove(pwfile)
            except Exception:
                pass
            if r.returncode != 0:
                # Algunas versiones de initdb (especialmente las portables de zonky en ciertos
                # sistemas) no soportan --pwfile y fallan. En ese caso reintentamos sin contraseña
                # y usamos pg_hba.conf con "trust" para la conexión local. La contraseña
                # todavía se puede configurar después con ALTER USER, pero para el uso local
                # de desarrollo esto es aceptable.
                r = subprocess.run(
                    [initdb, "-D", pg_data, "-U", user or "postgres"],
                    capture_output=True, encoding="utf-8", errors="replace", env=env, **su
                )
                if r.returncode != 0:
                    return {"ok": False, "error": f"initdb: {r.stderr.strip()}"}

        # Arrancar el servidor en el puerto asignado.
        # No usamos capture_output=True (pipes) para esta llamada: en Windows,
        # "pg_ctl -w start" deja vivo al proceso postgres (+ workers) como hijo
        # del propio pg_ctl, y esos procesos heredan el extremo de escritura de
        # los pipes de stdout/stderr. Como el server nunca cierra ese handle,
        # el read() de Python en subprocess.run() se queda esperando EOF para
        # siempre, aunque pg_ctl ya haya terminado. Redirigir a un archivo real
        # evita el problema (Python solo espera a que termine pg_ctl, no a que
        # se cierren handles heredados por los hijos).
        # En Windows, justo después de descomprimir binarios nuevos (o la primera
        # vez que se tocan), el antivirus puede escanearlos y retener brevemente
        # un lock sobre el archivo de log recién creado, haciendo que pg_ctl falle
        # con "no se pudo abrir el archivo de log: Permission denied" aunque los
        # permisos estén bien. Es transitorio: reintentar a los pocos ms alcanza.
        pg_ctl_out = os.path.join(data_dir, ".pg_ctl_start.log")
        output = ""
        for intento in range(3):
            with open(pg_ctl_out, "w", encoding="utf-8") as _lf:
                r = subprocess.run(
                    [pg_ctl, "-D", pg_data, "-l", log_file, "-o", f"-p {port}", "-w", "start"],
                    stdout=_lf, stderr=subprocess.STDOUT, env=env,
                    **self._su_kwargs(data_dir)
                )
            try:
                output = open(pg_ctl_out, encoding="utf-8", errors="replace").read()
            except Exception:
                output = ""
            if r.returncode == 0 or "no se pudo abrir el archivo de log" not in output.lower() and "could not open log file" not in output.lower():
                break
            time.sleep(0.8)
        if r.returncode == 0 or "server started" in output or "already running" in output:
            if database and database not in ("postgres", "template0", "template1"):
                # Esperar que el puerto esté listo antes de crear la BD target
                for _ in range(30):
                    try:
                        with socket.create_connection(("localhost", port), timeout=1):
                            break
                    except OSError:
                        time.sleep(1)
                if not _pg_create_database("localhost", port, user, password, database):
                    if self._pg_reconcile_and_create(pg_data, data_dir, port, user, password, database, env):
                        return {"ok": True}
                    return {"ok": False, "error": f"PostgreSQL arrancó pero no se pudo crear la base de datos '{database}' (autenticación falló incluso tras reconciliar la contraseña)"}
            return {"ok": True}
        return {"ok": False, "error": f"pg_ctl start: {output.strip()}"}

    def _stop_postgresql(self, data_dir: str) -> dict:
        pg_ctl  = self._bin("pg_ctl", "postgresql")
        pg_data = os.path.join(data_dir, "pgdata")
        if not os.path.isdir(pg_data) or not pg_ctl:
            return {"ok": True}
        # Leer el PID del postmaster ANTES de pararlo: postmaster.pid se
        # borra al pararse, y es el único enlace para encontrar a sus
        # workers después — los workers (--forkaux/--forkbgworker/etc) NO
        # llevan pg_data en su línea de comando, solo el postmaster la tiene.
        postmaster_pid = None
        pid_file = os.path.join(pg_data, "postmaster.pid")
        if os.path.exists(pid_file):
            try:
                postmaster_pid = int(open(pid_file).readline().strip())
            except Exception:
                postmaster_pid = None
        r = subprocess.run(
            [pg_ctl, "-D", pg_data, "stop", "-m", "fast"],
            capture_output=True, encoding="utf-8", errors="replace", env=self._proc_env("postgresql"),
            **self._su_kwargs(data_dir)
        )
        print(f"[DEBUG pg_ctl stop] pg_data={pg_data} returncode={r.returncode} stdout={r.stdout!r} stderr={r.stderr!r}", flush=True)
        # "not running" también es éxito → ya estaba parado
        ok = r.returncode == 0 or "not running" in (r.stdout + r.stderr)
        # pg_ctl stop manda la señal al postmaster y este a su vez para a sus
        # workers (checkpointer, bgwriter, autovacuum, etc.) — en teoría basta.
        # Pero si pg_ctl stop falla o queda a medias, esos workers quedan
        # huérfanos sin puerto propio: down() no los vuelve a ver nunca
        # porque no escuchan en ningún puerto rastreable. Barrida de
        # seguridad: matar al postmaster (si sigue vivo) y a cualquier
        # proceso cuyo ParentProcessId sea el postmaster — es el único
        # vínculo real con los workers en Windows, matchear por ruta en la
        # línea de comando no sirve porque los workers no la llevan.
        if _IS_WINDOWS and postmaster_pid:
            self._force_kill_children("postgres.exe", postmaster_pid)
        return {"ok": ok}

    @staticmethod
    def _force_kill_children(exe_name: str, parent_pid: int):
        """Mata al proceso `parent_pid` (si sigue vivo) y a cualquier
        `exe_name` cuyo ParentProcessId sea `parent_pid` — red de seguridad
        para workers huérfanos que un stop normal (pg_ctl, taskkill por
        puerto) no alcanza a ver porque no escuchan en ningún puerto propio
        ni llevan la ruta de datos en su línea de comando."""
        try:
            ps_cmd = (
                f"Get-CimInstance Win32_Process -Filter \"Name='{exe_name}'\" | "
                f"Where-Object {{ $_.ProcessId -eq {parent_pid} -or $_.ParentProcessId -eq {parent_pid} }} | "
                f"ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }}"
            )
            subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_cmd],
                capture_output=True, encoding="utf-8", errors="replace", timeout=15
            )
        except Exception:
            pass

    def _start_mysql(self, data_dir: str, port: int,
                     user: str = "dbuser", password: str = "dbpass",
                     database: str = "db", **_) -> dict:
        mysqld = self._bin("mysqld", "mysql")
        if not mysqld:
            return {"ok": False, "error": "MySQL no disponible. Instala con: sudo apt install mysql-server"}

        env         = self._proc_env("mysql")
        mysql_data  = os.path.join(data_dir, "data")
        log_file    = os.path.join(data_dir, "mysql.log")
        pid_file    = os.path.join(data_dir, "mysql.pid")
        socket_file = os.path.join(data_dir, "mysql.sock")

        # En contenedor root: mysqld hace su propia bajada de privilegios con --user=<uid>.
        # Le damos el uid sin privilegios y le hacemos chown del datadir.
        ids = self._unprivileged_ids()
        user_args = []
        if ids:
            uid, gid = ids
            self._su_kwargs(data_dir)  # chown recursivo del datadir
            user_args = [f"--user={uid}"]
        elif os.getenv("USER"):
            user_args = [f"--user={os.getenv('USER')}"]

        # Inicializar directorio de datos si es la primera vez
        if not os.path.isdir(mysql_data):
            os.makedirs(mysql_data, exist_ok=True)
            self._su_kwargs(data_dir)  # el datadir nuevo también debe ser del uid
            r = subprocess.run(
                [mysqld, "--initialize-insecure",
                 f"--datadir={mysql_data}"] + user_args,
                capture_output=True, encoding="utf-8", errors="replace", env=env
            )
            if r.returncode != 0:
                return {"ok": False, "error": f"mysqld --initialize: {r.stderr.strip()}"}

        # mysqld no tiene un comando "start" como pg_ctl; se lanza directamente como proceso.
        # Usamos Popen (no-bloqueante) en lugar de run() porque mysqld no hace fork solo
        # en Linux: el proceso padre se queda corriendo. Si usáramos run() bloquearíamos
        # el hilo del servidor hasta que mysqld se detenga.
        # stdout/stderr → DEVNULL porque los errores van al log_file especificado con --log-error.
        proc = subprocess.Popen(
            [mysqld, f"--datadir={mysql_data}", f"--port={port}",
             f"--socket={socket_file}", f"--pid-file={pid_file}",
             f"--log-error={log_file}"] + user_args,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
        )
        return {"ok": True, "pid": proc.pid}

    def _stop_mysql(self, data_dir: str, **_) -> dict:
        pid_file = os.path.join(data_dir, "mysql.pid")
        if os.path.exists(pid_file):
            try:
                pid = int(open(pid_file).read().strip())
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        return {"ok": True}

    def _start_mongodb(self, data_dir: str, port: int,
                       user: str = "", password: str = "",
                       database: str = "", **_) -> dict:
        mongod = self._bin("mongod", "mongodb")
        if not mongod:
            return {"ok": False, "error": "MongoDB no disponible. Instala con: sudo apt install mongodb"}

        env        = self._proc_env("mongodb")
        mongo_data = os.path.join(data_dir, "data")
        log_file   = os.path.join(data_dir, "mongod.log")
        pid_file   = os.path.join(data_dir, "mongod.pid")
        os.makedirs(mongo_data, exist_ok=True)

        # Verificar si ya hay un proceso vivo antes de lanzar otro
        if os.path.exists(pid_file):
            try:
                pid = int(open(pid_file).read().strip())
                os.kill(pid, 0)
                return {"ok": True}  # ya está corriendo
            except (ProcessLookupError, PermissionError):
                pass
            except Exception:
                pass
            try:
                os.remove(pid_file)
            except Exception:
                pass

        cmd = [mongod,
               f"--dbpath={mongo_data}", f"--port={port}",
               f"--logpath={log_file}", f"--pidfilepath={pid_file}",
               "--fork"]  # mongod con --fork hace el fork() él mismo y el padre termina.
                          # Esto nos permite usar subprocess.run() (bloqueante) en lugar de Popen,
                          # porque el proceso hijo que queda corriendo NO es el que run() espera.
        if user and password:
            cmd.append("--auth")

        r = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace", env=env)
        if r.returncode != 0 and "already running" not in (r.stdout + r.stderr):
            return {"ok": False, "error": f"mongod: {r.stderr.strip()}"}
        return {"ok": True}

    def _stop_mongodb(self, data_dir: str, **_) -> dict:
        pid_file = os.path.join(data_dir, "mongod.pid")
        if os.path.exists(pid_file):
            try:
                pid = int(open(pid_file).read().strip())
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        return {"ok": True}

    def _start_redis(self, data_dir: str, port: int,
                     password: str = "", **_) -> dict:
        redis_server = self._bin("redis-server", "redis")
        if not redis_server:
            return {"ok": False, "error": "Redis no disponible. Instala con: sudo apt install redis-server"}

        env      = self._proc_env("redis")
        log_file = os.path.join(data_dir, "redis.log")
        pid_file = os.path.join(data_dir, "redis.pid")

        # Verificar si ya está corriendo
        if os.path.exists(pid_file):
            try:
                pid = int(open(pid_file).read().strip())
                os.kill(pid, 0)
                return {"ok": True}
            except (ProcessLookupError, PermissionError):
                pass
            except Exception:
                pass
            try:
                os.remove(pid_file)
            except Exception:
                pass

        cmd = [redis_server,
               "--port", str(port),
               "--daemonize", "yes",   # correr en background
               "--logfile", log_file,
               "--pidfile", pid_file,
               "--dir", data_dir]
        if password:
            cmd += ["--requirepass", password]

        r = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace", env=env)
        if r.returncode != 0:
            return {"ok": False, "error": f"redis-server: {r.stderr.strip()}"}
        return {"ok": True}

    def _stop_redis(self, data_dir: str, port: int = 6379, password: str = "", **_) -> dict:
        # Redis prefiere el comando "SHUTDOWN" (vía redis-cli) sobre SIGTERM porque:
        #   1. Hace BGSAVE antes de apagarse → los datos en disco quedan consistentes.
        #   2. El proceso se cierra limpiamente después del save.
        # SIGTERM también funciona pero puede perder datos del último segundo si Redis
        # no ha terminado de escribir el RDB/AOF en disco.
        redis_cli = self._bin("redis-cli", "redis")
        if redis_cli:
            cmd = [redis_cli, "-p", str(port)]
            if password:
                cmd += ["-a", password]
            cmd.append("shutdown")
            try:
                subprocess.run(cmd, capture_output=True, timeout=5)
                return {"ok": True}
            except Exception:
                pass
        # Si redis-cli no está disponible o el proceso ya murió, fallback a SIGTERM por PID.
        pid_file = os.path.join(data_dir, "redis.pid")
        if os.path.exists(pid_file):
            try:
                pid = int(open(pid_file).read().strip())
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        return {"ok": True}

    # ── Interfaz pública (espeja DockerManager) ───────────────────────────────

    def generate_compose(self, project_path: str, db_type: str,
                         credentials: dict, ports: dict) -> str:
        # El Gestor (orchestrator.py) llama a self.docker.generate_compose() sin saber
        # si self.docker es un DockerManager o un NativeManager. Para mantener la misma
        # interfaz, NativeManager genera un .orq_native.json en lugar de un docker-compose.yml.
        # El resto del código en el Gestor trata este JSON como si fuera un compose file
        # (lo pasa a get_compose_db_info(), get_running_ports(), etc.) y esos métodos
        # en NativeManager saben cómo leerlo.
        config = {
            "db_type": db_type,
            "port": ports.get("db_port") or credentials.get("port") or _CONTAINER_PORTS.get(db_type, 5432),
            "credentials": credentials,
        }
        config_path = self._config_path(project_path)
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
            return config_path
        except Exception:
            return ""

    def up(self, project_path: str, network_name: str, port_mapping: dict,
           only_service: str | None = None) -> dict:
        # only_service: existe solo para compatibilidad con la firma de
        # DockerManager.up() (que arranca un único servicio de compose). En
        # modo nativo/portable no hay compose — cada config corre un único
        # proceso de BD, así que el parámetro se ignora.
        config = self._load_config(project_path)

        # Si no existe .orq_native.json, el proyecto puede tener un docker-compose.yml
        # del desarrollador original. Lo leemos para reutilizar sus credenciales y tipo de BD
        # en lugar de fallar o pedir al usuario que configure todo desde cero.
        # _parse_compose_to_config usa regex (no un parser YAML completo) porque no queremos
        # depender de PyYAML como dependencia. El 95% de los compose.yml usa sintaxis simple
        # que regex maneja correctamente para extraer imagen, puerto y variables de entorno.
        if not config:
            compose_names = ("docker-compose.yml", "docker-compose.yaml",
                             "compose.yml", "compose.yaml")
            for name in compose_names:
                cf = os.path.join(project_path, name)
                if os.path.isfile(cf):
                    db_type = self._detect_db_type_from_compose(cf)
                    if db_type:
                        config = _parse_compose_to_config(cf, db_type)
                        if config:
                            # Persistir a .orq_native.json: down() SOLO lee de ahí
                            # (nunca re-parsea el compose.yml), así que si esto no
                            # se guarda, down() siempre ve {} y hace un no-op
                            # silencioso — "ok:true" sin parar nada de verdad.
                            try:
                                with open(self._config_path(project_path), "w", encoding="utf-8") as f:
                                    json.dump(config, f, indent=2)
                            except Exception:
                                pass
                    break

        if not config or not config.get("db_type"):
            return {"ok": False, "error": "No hay configuración nativa. No se encontró .orq_native.json ni docker-compose.yml válido"}

        db_type  = config.get("db_type")
        port     = port_mapping.get("db_port") or config.get("port") or _CONTAINER_PORTS.get(db_type, 5432)
        creds    = config.get("credentials", {})

        data_dir = self._data_dir(project_path, db_type)

        # Tabla de dispatch en lugar de if/elif para que agregar un nuevo tipo de BD
        # sea añadir una sola línea aquí y una función _start_X, sin tocar la lógica de up().
        starters = {
            "postgresql": self._start_postgresql,
            "mysql":      self._start_mysql,
            "mariadb":    self._start_mysql,  # MariaDB es binariamente compatible con MySQL; mysqld arranca ambas
            "mongodb":    self._start_mongodb,
            "redis":      self._start_redis,
        }
        fn = starters.get(db_type)
        if not fn:
            return {"ok": False, "error": f"DB tipo '{db_type}' no soportado en modo nativo"}

        return fn(data_dir, int(port), **{
            "user":     creds.get("user", "dbuser"),
            "password": creds.get("password", "dbpass"),
            "database": creds.get("database", db_type),
        })

    def down(self, project_path: str) -> dict:
        config = self._load_config(project_path)
        if not config:
            return {"ok": True}

        db_type  = config.get("db_type")
        port     = int(config.get("port") or _CONTAINER_PORTS.get(db_type, 0))
        creds    = config.get("credentials", {})
        password = creds.get("password", "")

        if not db_type:
            return {"ok": True}

        data_dir = self._data_dir(project_path, db_type)

        stoppers = {
            "postgresql": self._stop_postgresql,
            "mysql":      self._stop_mysql,
            "mariadb":    self._stop_mysql,
            "mongodb":    self._stop_mongodb,
            "redis":      self._stop_redis,
        }
        fn = stoppers.get(db_type)
        if not fn:
            return {"ok": True}

        # Redis necesita port y password para el shutdown limpio
        kwargs = {"port": port, "password": password}
        return fn(data_dir, **kwargs) if db_type == "redis" else fn(data_dir)

    def status(self, project_path: str) -> dict:
        config = self._load_config(project_path)
        if not config:
            return {}
        db_type = config.get("db_type")
        port    = config.get("port")
        if not db_type or not port:
            return {}
        # Comprobamos el estado intentando abrir una conexión TCP al puerto de la BD.
        # Es más fiable que leer el PID file porque: (a) el PID puede ser stale si el proceso
        # murió sin limpiarlo, (b) el proceso puede estar vivo pero no aceptar conexiones todavía.
        # Si el socket abre → el servidor está listo para recibir queries.
        try:
            with socket.create_connection(("localhost", int(port)), timeout=0.5):
                return {f"{db_type}_native": "running"}
        except OSError:
            return {f"{db_type}_native": "stopped"}

    def get_compose_db_info(self, compose_file: str, db_type: str) -> dict:
        # compose_file puede ser .orq_native.json (nativo) o un compose.yml real
        try:
            with open(compose_file, encoding="utf-8") as f:
                config = json.load(f)
            # Es nuestro JSON de config nativa
            creds = config.get("credentials", {})
            info: dict = {}
            if config.get("port"):
                info["host_port"] = int(config["port"])
            for key in ("user", "password", "database"):
                if creds.get(key):
                    info[key] = creds[key]
            return info
        except (json.JSONDecodeError, Exception):
            # Fallback: es un docker-compose.yml → parsear con regex
            return _parse_compose_to_config(compose_file, db_type).get("credentials", {})

    def get_running_ports(self, compose_file: str) -> dict:
        try:
            with open(compose_file, encoding="utf-8") as f:
                config = json.load(f)
            db_type = config.get("db_type")
            port    = config.get("port")
            if db_type and port:
                container_port = _CONTAINER_PORTS.get(db_type)
                if container_port:
                    return {container_port: int(port)}
        except Exception:
            pass
        return {}

    def wait_for_port(self, host: str, port: int, timeout: int = 30) -> bool:
        for _ in range(timeout):
            try:
                with socket.create_connection((host, int(port)), timeout=1):
                    return True
            except OSError:
                time.sleep(1)
        return False

    # ── Helpers internos ──────────────────────────────────────────────────────

    def _detect_db_type_from_compose(self, compose_file: str) -> str:
        """Detecta el tipo de BD de un compose.yml buscando el nombre de la imagen Docker."""
        import re
        try:
            with open(compose_file, encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            return ""
        patterns = {
            "postgresql": r"image:\s*postgres",
            "mysql":      r"image:\s*mysql",
            "mariadb":    r"image:\s*mariadb",
            "mongodb":    r"image:\s*mongo",
            "redis":      r"image:\s*redis",
        }
        for db_type, pattern in patterns.items():
            if re.search(pattern, content, re.IGNORECASE):
                return db_type
        return ""

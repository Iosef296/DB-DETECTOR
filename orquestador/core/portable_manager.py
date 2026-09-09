"""
PortableManager: descarga binarios portables de cada BD al primer uso.
Sin instalación del sistema. Internet solo en la primera descarga por BD.

Fuentes de binarios:
  PostgreSQL → zonky.io (Maven Central) — JAR que contiene tar.xz con binarios precompilados
  Redis      → Valkey releases (GitHub) — fork oficial 100% compatible con Redis
  MongoDB    → fastdl.mongodb.org       — .tgz oficial de MongoDB
  MySQL      → dev.mysql.com            — .tar.xz oficial de MySQL

Estructura en disco:
  ~/.orquestador/bins/
    postgresql/  ← binarios extraídos del JAR de zonky
    redis/       ← binarios de Valkey
    mongodb/     ← binarios de MongoDB
    mysql/       ← binarios de MySQL
"""

import io
import os
import stat
import shutil
import platform
import tarfile
import zipfile
import urllib.request

from core.native_manager import NativeManager
from core.paths import data_root

_PG_VERSION = "16.4.0"


def _bins_dir() -> str:
    # Directorio base de los binarios descargados. Se resuelve en cada llamada
    # para respetar ORQ_DATA_DIR (volumen de Railway) aunque cambie tras el import.
    path = os.path.join(data_root(), "bins")
    os.makedirs(path, exist_ok=True)
    return path


# ── Detección de plataforma ────────────────────────────────────────────────────

def _platform() -> tuple[str, str]:
    """Devuelve (sistema, arquitectura) normalizados para el catálogo."""
    system  = platform.system().lower()   # "linux", "darwin", "windows"
    machine = platform.machine().lower()  # "x86_64", "aarch64", "arm64"
    if machine in ("aarch64", "arm64"):
        machine = "arm64"
    else:
        machine = "x86_64"
    return system, machine


# ── Catálogo de descargas ──────────────────────────────────────────────────────
# Estructura: {sistema: {arquitectura: {db_type: {url, format, bin_paths, lib_path?}}}}
# - format: "tar" | "zonky_jar"
# - bin_paths: {nombre_lógico → ruta_relativa_dentro_del_directorio_extraído}
# - lib_path: ruta a las librerías compartidas (para LD_LIBRARY_PATH)

_CATALOG: dict[str, dict[str, dict]] = {
    "linux": {
        "x86_64": {
            # PostgreSQL via zonky.io (Maven Central) — binarios muy estables y probados
            "postgresql": {
                "url": f"https://repo1.maven.org/maven2/io/zonky/test/postgres/"
                       f"embedded-postgres-binaries-linux-amd64/{_PG_VERSION}/"
                       f"embedded-postgres-binaries-linux-amd64-{_PG_VERSION}.jar",
                "format": "zonky_jar",  # JAR = ZIP que contiene un .txz con los binarios
                "bin_paths": {
                    "initdb":   "bin/initdb",
                    "pg_ctl":   "bin/pg_ctl",
                    "postgres": "bin/postgres",
                },
                "lib_path": "lib",  # librerías shared necesarias en LD_LIBRARY_PATH
            },
            # Valkey = fork oficial de Redis con releases binarios en GitHub
            "redis": {
                "url": "https://github.com/valkey-io/valkey/releases/download/7.2.7/valkey-7.2.7-linux-x86_64.tar.gz",
                "format": "tar",
                "bin_paths": {
                    "redis-server": "valkey-7.2.7-linux-x86_64/bin/valkey-server",
                    "redis-cli":    "valkey-7.2.7-linux-x86_64/bin/valkey-cli",
                },
            },
            "mongodb": {
                "url": "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian12-7.0.14.tgz",
                "format": "tar",
                "bin_paths": {
                    "mongod": "mongodb-linux-x86_64-debian12-7.0.14/bin/mongod",
                },
            },
            "mysql": {
                "url": "https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.36-linux-glibc2.28-x86_64.tar.xz",
                "format": "tar",
                "bin_paths": {
                    "mysqld": "mysql-8.0.36-linux-glibc2.28-x86_64/bin/mysqld",
                    "mysql":  "mysql-8.0.36-linux-glibc2.28-x86_64/bin/mysql",
                },
                "lib_path": "mysql-8.0.36-linux-glibc2.28-x86_64/lib",
            },
        },
        "arm64": {
            "postgresql": {
                "url": f"https://repo1.maven.org/maven2/io/zonky/test/postgres/"
                       f"embedded-postgres-binaries-linux-arm64v8/{_PG_VERSION}/"
                       f"embedded-postgres-binaries-linux-arm64v8-{_PG_VERSION}.jar",
                "format": "zonky_jar",
                "bin_paths": {
                    "initdb":   "bin/initdb",
                    "pg_ctl":   "bin/pg_ctl",
                    "postgres": "bin/postgres",
                },
                "lib_path": "lib",
            },
            "redis": {
                "url": "https://github.com/valkey-io/valkey/releases/download/7.2.7/valkey-7.2.7-linux-arm64.tar.gz",
                "format": "tar",
                "bin_paths": {
                    "redis-server": "valkey-7.2.7-linux-arm64/bin/valkey-server",
                    "redis-cli":    "valkey-7.2.7-linux-arm64/bin/valkey-cli",
                },
            },
            "mongodb": {
                "url": "https://fastdl.mongodb.org/linux/mongodb-linux-aarch64-debian12-7.0.14.tgz",
                "format": "tar",
                "bin_paths": {
                    "mongod": "mongodb-linux-aarch64-debian12-7.0.14/bin/mongod",
                },
            },
        },
    },
    "darwin": {
        "x86_64": {
            "postgresql": {
                "url": f"https://repo1.maven.org/maven2/io/zonky/test/postgres/"
                       f"embedded-postgres-binaries-darwin-amd64/{_PG_VERSION}/"
                       f"embedded-postgres-binaries-darwin-amd64-{_PG_VERSION}.jar",
                "format": "zonky_jar",
                "bin_paths": {
                    "initdb":   "bin/initdb",
                    "pg_ctl":   "bin/pg_ctl",
                    "postgres": "bin/postgres",
                },
                "lib_path": "lib",
            },
            "redis": {
                "url": "https://github.com/valkey-io/valkey/releases/download/7.2.7/valkey-7.2.7-macos-x86_64.tar.gz",
                "format": "tar",
                "bin_paths": {
                    "redis-server": "valkey-7.2.7-macos-x86_64/bin/valkey-server",
                    "redis-cli":    "valkey-7.2.7-macos-x86_64/bin/valkey-cli",
                },
            },
        },
        "arm64": {
            "postgresql": {
                "url": f"https://repo1.maven.org/maven2/io/zonky/test/postgres/"
                       f"embedded-postgres-binaries-darwin-arm64v8/{_PG_VERSION}/"
                       f"embedded-postgres-binaries-darwin-arm64v8-{_PG_VERSION}.jar",
                "format": "zonky_jar",
                "bin_paths": {
                    "initdb":   "bin/initdb",
                    "pg_ctl":   "bin/pg_ctl",
                    "postgres": "bin/postgres",
                },
                "lib_path": "lib",
            },
            "redis": {
                "url": "https://github.com/valkey-io/valkey/releases/download/7.2.7/valkey-7.2.7-macos-arm64.tar.gz",
                "format": "tar",
                "bin_paths": {
                    "redis-server": "valkey-7.2.7-macos-arm64/bin/valkey-server",
                    "redis-cli":    "valkey-7.2.7-macos-arm64/bin/valkey-cli",
                },
            },
        },
    },
}


# ── Helpers de descarga y extracción ──────────────────────────────────────────

def _download(url: str, dest: str, on_progress=None):
    # Crear directorio de destino si no existe
    os.makedirs(os.path.dirname(dest), exist_ok=True)

    def _hook(block, block_size, total):
        # Callback de progreso: llamar solo si el total es conocido
        if on_progress and total > 0:
            on_progress(min(block * block_size, total), total)

    urllib.request.urlretrieve(url, dest, _hook)


def _mark_executable(path: str):
    # Marcar todos los archivos dentro del directorio como ejecutables
    for root, _, files in os.walk(path):
        for fname in files:
            fpath = os.path.join(root, fname)
            try:
                st = os.stat(fpath)
                os.chmod(fpath, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
            except Exception:
                pass


def _extract_tar(archive: str, dest_dir: str):
    os.makedirs(dest_dir, exist_ok=True)
    with tarfile.open(archive) as t:
        t.extractall(dest_dir)
    _mark_executable(dest_dir)


def _extract_zonky_jar(jar_path: str, dest_dir: str):
    """
    Los JARs de zonky son ZIPs que contienen un único archivo .txz (tar.xz).
    Ese .txz contiene los binarios de PostgreSQL en bin/, lib/, share/.
    Se lee el ZIP en memoria y se extrae el tar.xz directamente sin escribir el .txz a disco.
    """
    os.makedirs(dest_dir, exist_ok=True)
    with zipfile.ZipFile(jar_path, "r") as z:
        # Buscar el único archivo .txz o .tar.xz dentro del JAR
        txz_names = [n for n in z.namelist()
                     if n.endswith(".txz") or n.endswith(".tar.xz")]
        if not txz_names:
            raise RuntimeError(f"No se encontró .txz dentro del JAR: {jar_path}")
        txz_data = z.read(txz_names[0])  # leer en memoria

    # Extraer el tar.xz desde bytes en memoria (sin archivo temporal en disco)
    with tarfile.open(fileobj=io.BytesIO(txz_data)) as t:
        t.extractall(dest_dir)

    _mark_executable(dest_dir)


def _extract(archive: str, dest_dir: str, fmt: str):
    if fmt == "zonky_jar":
        _extract_zonky_jar(archive, dest_dir)
    else:
        _extract_tar(archive, dest_dir)


# ── PortableManager ───────────────────────────────────────────────────────────

class PortableManager(NativeManager):
    """
    Extiende NativeManager con auto-descarga de binarios portables al primer uso.
    Caché en ~/.orquestador/bins/. Internet solo en la primera descarga por tipo de BD.
    Después de la primera descarga, funciona completamente offline.
    """

    # ── Búsqueda en catálogo ──────────────────────────────────────────────────

    def _release(self, db_type: str) -> dict | None:
        system, machine = _platform()
        return _CATALOG.get(system, {}).get(machine, {}).get(db_type)

    def _bin_dir(self, db_type: str) -> str:
        return os.path.join(_bins_dir(), db_type)

    def _portable_bin(self, db_type: str, name: str) -> str | None:
        """Devuelve la ruta completa al binario portable si ya está descargado y es ejecutable."""
        release = self._release(db_type)
        if not release:
            return None
        rel_path = release["bin_paths"].get(name)
        if not rel_path:
            return None
        full = os.path.join(self._bin_dir(db_type), rel_path)
        # Verificamos os.access(..., os.X_OK) además de isfile() porque en algunos sistemas
        # de archivos (FAT, algunos NFS) el bit ejecutable se pierde al extraer el tar.
        # Si no es ejecutable, _mark_executable() debería haberlo arreglado, pero si falló
        # preferimos volver a descargar que lanzar un "Permission denied" confuso al usuario.
        return full if os.path.isfile(full) and os.access(full, os.X_OK) else None

    # ── Overrides de NativeManager ────────────────────────────────────────────

    def _bin(self, name: str, db_type: str = "") -> str | None:
        # Sobreescribimos el _bin() de NativeManager para insertar una capa de prioridad:
        # primero el binario portable (ruta absoluta, controlada por nosotros),
        # luego el del sistema (shutil.which). Esto evita que una versión incompatible
        # instalada en el sistema interfiera con los binarios que descargamos nosotros.
        if db_type:
            portable = self._portable_bin(db_type, name)
            if portable:
                return portable
        return shutil.which(name)

    def _proc_env(self, db_type: str) -> dict:
        # PostgreSQL y MySQL portables traen librerías compartidas (.so) propias.
        # Sin LD_LIBRARY_PATH apuntando a esas librerías el proceso lanza:
        #   "error while loading shared libraries: libssl.so.X: cannot open shared object file"
        # Heredamos el entorno completo del sistema (os.environ.copy()) para no romper
        # PATH, HOME, USER y otras variables que los procesos de BD necesitan implícitamente.
        env = os.environ.copy()
        release = self._release(db_type)
        if release and release.get("lib_path"):
            lib_dir = os.path.join(self._bin_dir(db_type), release["lib_path"])
            existing = env.get("LD_LIBRARY_PATH", "")
            # Anteponer nuestra lib_path para que tenga prioridad sobre las del sistema,
            # evitando conflictos de versión de libssl/libcrypto entre el sistema y PostgreSQL.
            env["LD_LIBRARY_PATH"] = f"{lib_dir}:{existing}" if existing else lib_dir
        return env

    # ── API de descarga ───────────────────────────────────────────────────────

    def ensure_db(self, db_type: str, on_progress=None) -> dict:
        """
        Descarga y extrae binarios portables para db_type si no están en caché.
        on_progress(bytes_descargados, total_bytes) es opcional.
        """
        release = self._release(db_type)
        if not release:
            system, machine = _platform()
            return {"ok": False, "error": f"No hay binarios portables para {db_type} en {system}/{machine}"}

        # Solo verificamos el PRIMER binario del catálogo como señal de que la descarga
        # fue completa. Si solo ese archivo existe, asumimos que el resto también está
        # (fueron extraídos del mismo archive en una sola operación atómica).
        first_rel = next(iter(release["bin_paths"].values()))
        first_bin = os.path.join(self._bin_dir(db_type), first_rel)
        if os.path.isfile(first_bin) and os.access(first_bin, os.X_OK):
            return {"ok": True, "cached": True}

        url     = release["url"]
        fmt     = release.get("format", "tar")
        fname   = url.rsplit("/", 1)[-1]
        archive = os.path.join(self._bin_dir(db_type), fname)

        try:
            _download(url, archive, on_progress)
            _extract(archive, self._bin_dir(db_type), fmt)
            # Borramos el archive descargado después de extraer para recuperar espacio:
            # PostgreSQL portable pesa ~80 MB comprimido, MySQL ~200 MB.
            # Los binarios extraídos ya son suficientes para arrancar la BD.
            try:
                os.remove(archive)
            except Exception:
                pass
            return {"ok": True, "cached": False}
        except Exception as e:
            return {"ok": False, "error": f"Descarga fallida para {db_type}: {e}"}

    def available_dbs(self) -> list[str]:
        """Tipos de BD disponibles en el catálogo para esta plataforma."""
        system, machine = _platform()
        return list(_CATALOG.get(system, {}).get(machine, {}).keys())

    def cached_dbs(self) -> list[str]:
        """Tipos de BD que ya están descargados y listos para usar."""
        result = []
        for db_type in self.available_dbs():
            release = self._release(db_type)
            if not release:
                continue
            first_rel = next(iter(release["bin_paths"].values()))
            first_bin = os.path.join(self._bin_dir(db_type), first_rel)
            if os.path.isfile(first_bin) and os.access(first_bin, os.X_OK):
                result.append(db_type)
        return result

    # ── Override de up(): auto-descargar antes de iniciar ────────────────────

    def up(self, project_path: str, network_name: str, port_mapping: dict) -> dict:
        config = self._load_config(project_path)
        if not config:
            # El proyecto puede tener un docker-compose.yml original (del desarrollador)
            # aunque nosotros no usemos Docker. Lo parseamos para reutilizar la config de
            # credenciales y tipo de BD en lugar de pedirle al usuario que la re-ingrese.
            for name in ("docker-compose.yml", "docker-compose.yaml",
                         "compose.yml", "compose.yaml"):
                cf = os.path.join(project_path, name)
                if os.path.isfile(cf):
                    db_type = self._detect_db_type_from_compose(cf)
                    if db_type:
                        from core.native_manager import _parse_compose_to_config
                        config = _parse_compose_to_config(cf, db_type)
                    break

        db_type = config.get("db_type") if config else None
        if db_type:
            # ensure_db() descarga solo si no está en caché, por eso es seguro llamarlo
            # en cada up(). Si los binarios ya existen, retorna inmediatamente {"ok": True, "cached": True}.
            # Si falla la descarga (sin internet, URL caída), propagamos el error aquí
            # antes de intentar arrancar un proceso que definitivamente va a fallar.
            result = self.ensure_db(db_type)
            if not result.get("ok"):
                return result

        # Una vez garantizados los binarios, delegamos todo el ciclo de vida al NativeManager.
        # PortableManager no reimplementa el arranque: solo añade la capa de descarga.
        # NativeManager._bin() será sobrescrito por nuestro _bin() de arriba, así que
        # usará los binarios portables que acabamos de asegurar.
        return super().up(project_path, network_name, port_mapping)

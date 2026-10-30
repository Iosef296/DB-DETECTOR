"""Arranque nativo/portable de infraestructura típica de microservicios
(Kafka, Keycloak) sin depender de Docker.

Mismo patrón que PortableManager usa para bases de datos: el binario oficial
se descarga una sola vez a ~/.orquestador/bins/infra/<nombre>/ y de ahí en
adelante corre como proceso nativo. Nada se instala a nivel de sistema.

Muchos monorepos de microservicios (este incluido) ya traen un compose-dev.yml
paralelo al compose.yml de producción: el "-dev" expone los servicios en
localhost con puertos fijos, pensado justamente para que apps corridas fuera
de Docker (ej. `mvnw spring-boot:run` en el IDE) se conecten a esa infra sin
tocar nada. Por eso: preferimos leer el compose-dev.yml (si existe) para
sacar puertos/credenciales, y el proceso nativo escucha en esos mismos
puertos — así el perfil "dev" que ya trae el proyecto encaja sin cambios.
"""
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import tarfile
import time
import zipfile

from core.paths import data_root
from core.portable_manager import _download, _grant_user_access, _mark_executable

_IS_WINDOWS = platform.system() == "Windows"

_KAFKA_VERSION = "4.1.0"
_KAFKA_SCALA = "2.13"
_KEYCLOAK_VERSION = "26.8.0"
_PROMETHEUS_VERSION = "2.54.1"
_LOKI_VERSION = "3.1.1"
_GRAFANA_VERSION = "11.2.0"
_KAFBAT_VERSION = "1.2.0"  # 1.5.0 exige JDK 25 (class file version 69); la mayoría tiene JDK 21

if _IS_WINDOWS:
    _PROMETHEUS_URL = f"https://github.com/prometheus/prometheus/releases/download/v{_PROMETHEUS_VERSION}/prometheus-{_PROMETHEUS_VERSION}.windows-amd64.zip"
    _PROMETHEUS_FMT = "zip"
    _LOKI_URL = f"https://github.com/grafana/loki/releases/download/v{_LOKI_VERSION}/loki-windows-amd64.exe.zip"
    _PROMTAIL_URL = f"https://github.com/grafana/loki/releases/download/v{_LOKI_VERSION}/promtail-windows-amd64.exe.zip"
    _LOKI_BIN = "loki-windows-amd64.exe"
    _PROMTAIL_BIN = "promtail-windows-amd64.exe"
    _GRAFANA_URL = f"https://dl.grafana.com/oss/release/grafana-{_GRAFANA_VERSION}.windows-amd64.zip"
    _GRAFANA_FMT = "zip"
else:
    _PROMETHEUS_URL = f"https://github.com/prometheus/prometheus/releases/download/v{_PROMETHEUS_VERSION}/prometheus-{_PROMETHEUS_VERSION}.linux-amd64.tar.gz"
    _PROMETHEUS_FMT = "tar"
    _LOKI_URL = f"https://github.com/grafana/loki/releases/download/v{_LOKI_VERSION}/loki-linux-amd64.zip"
    _PROMTAIL_URL = f"https://github.com/grafana/loki/releases/download/v{_LOKI_VERSION}/promtail-linux-amd64.zip"
    _LOKI_BIN = "loki-linux-amd64"
    _PROMTAIL_BIN = "promtail-linux-amd64"
    _GRAFANA_URL = f"https://dl.grafana.com/oss/release/grafana-{_GRAFANA_VERSION}.linux-amd64.tar.gz"
    _GRAFANA_FMT = "tar"

_CATALOG = {
    "kafka": {
        # downloads.apache.org solo sirve las últimas versiones (rota al salir
        # releases nuevos); archive.apache.org guarda todas para siempre.
        "url": f"https://archive.apache.org/dist/kafka/{_KAFKA_VERSION}/kafka_{_KAFKA_SCALA}-{_KAFKA_VERSION}.tgz",
        "format": "tar",
        "home_name": f"kafka_{_KAFKA_SCALA}-{_KAFKA_VERSION}",
        "default_port": 9092,
    },
    "keycloak": {
        "url": f"https://github.com/keycloak/keycloak/releases/download/{_KEYCLOAK_VERSION}/keycloak-{_KEYCLOAK_VERSION}.zip",
        "format": "zip",
        "home_name": f"keycloak-{_KEYCLOAK_VERSION}",
        "default_port": 8080,
    },
    "prometheus": {
        "url": _PROMETHEUS_URL,
        "format": _PROMETHEUS_FMT,
        "home_name": f"prometheus-{_PROMETHEUS_VERSION}." + ("windows-amd64" if _IS_WINDOWS else "linux-amd64"),
        "default_port": 9090,
    },
    "loki": {
        "kind": "flat",
        "url": _LOKI_URL,
        "bin_name": _LOKI_BIN,
        "default_port": 3100,
    },
    "promtail": {
        "kind": "flat",
        "url": _PROMTAIL_URL,
        "bin_name": _PROMTAIL_BIN,
        "default_port": 9080,
    },
    "grafana": {
        "url": _GRAFANA_URL,
        "format": _GRAFANA_FMT,
        "home_name": f"grafana-v{_GRAFANA_VERSION}",
        "default_port": 3000,
    },
    "kafbat": {
        # UI web de administración de Kafka (fork de provectus/kafka-ui).
        # Se arranca junto con Kafka (ver _up_kafka_bundle) para que el
        # "app_url" que ve el usuario sea esta UI navegable y no el puerto
        # crudo del broker (protocolo binario, sin nada que abrir en navegador).
        "kind": "jar",
        "url": f"https://github.com/kafbat/kafka-ui/releases/download/v{_KAFBAT_VERSION}/api-v{_KAFBAT_VERSION}.jar",
        "bin_name": f"kafbat-ui-{_KAFBAT_VERSION}.jar",
        "default_port": 8090,
    },
}

# Nombres con los que detector.py puede identificar la carpeta de obs
# (ver detect_infra_service: por match de imagen en el compose, puede
# devolver "prometheus" o "grafana" en vez de "obs" si están todos juntos
# en un solo compose.yml). Todos arrancan el mismo bundle completo.
_OBS_NAMES = ("prometheus", "loki", "promtail", "grafana", "obs")


def _infra_bins_dir() -> str:
    d = os.path.join(data_root(), "bins", "infra")
    os.makedirs(d, exist_ok=True)
    return d


def _infra_data_dir(infra_name: str, project_path: str) -> str:
    key = os.path.basename(project_path.rstrip("/\\")).lower() or "default"
    d = os.path.join(data_root(), "infra_data", infra_name, key)
    os.makedirs(d, exist_ok=True)
    return d


def _extract_zip(archive: str, dest_dir: str):
    os.makedirs(dest_dir, exist_ok=True)
    with zipfile.ZipFile(archive) as z:
        z.extractall(dest_dir)
    _mark_executable(dest_dir)


def _extract_tgz(archive: str, dest_dir: str):
    os.makedirs(dest_dir, exist_ok=True)
    with tarfile.open(archive) as t:
        t.extractall(dest_dir)
    _mark_executable(dest_dir)


def _wait_for_port(host: str, port: int, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _is_running(pid: int) -> bool:
    if not pid or pid <= 0:
        return False
    if _IS_WINDOWS:
        try:
            out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                                  capture_output=True, text=True, timeout=10)
            return str(pid) in out.stdout
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _kill(pid: int):
    if not pid or pid <= 0:
        return
    try:
        if _IS_WINDOWS:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                            capture_output=True, timeout=15)
        else:
            os.kill(pid, 15)
    except Exception:
        pass


def _parse_compose_services(content: str) -> dict:
    """Mismo patrón que docker_manager._find_db_service_name: recorta el
    bloque `services:` y devuelve {nombre_servicio: cuerpo_yaml_crudo}."""
    services_block = re.search(r'^services:\s*\n((?:(?:[ \t]+.*)?\n)+)', content, re.MULTILINE)
    if not services_block:
        return {}
    block = services_block.group(1)
    out = {}
    for m in re.finditer(r'^[ \t]{2}(\S+):\s*\n((?:(?:[ \t]{3,}.*)?\n)*)', block, re.MULTILINE):
        out[m.group(1)] = m.group(2)
    return out


def _classify_obs_service(svc_body: str):
    img = re.search(r'image:\s*["\']?(\S+)', svc_body)
    if not img:
        return None
    image = img.group(1).lower()
    if "prometheus" in image:
        return "prometheus"
    if "promtail" in image:
        return "promtail"
    if "loki" in image:
        return "loki"
    if "grafana" in image:
        return "grafana"
    return None


def _service_volumes(svc_body: str):
    """[(host_path_rel, container_path), ...] de las líneas `- host:container[:ro]`."""
    out = []
    for line in svc_body.splitlines():
        m = re.match(r'^\s*-\s*([^:\s][^:]*):(/[^:\s]+)(?::\w+)?\s*$', line)
        if m:
            out.append((m.group(1), m.group(2)))
    return out


def _service_host_port(svc_body: str, container_port: int):
    m = re.search(rf'["\']?(\d{{2,5}}):{container_port}\b', svc_body)
    return int(m.group(1)) if m else None


def _config_mount_host_path(volumes, container_suffix: str):
    for host_rel, container_path in volumes:
        if container_path.endswith(container_suffix):
            return host_rel
    return None


def _dir_mount_host_path(volumes, container_dir: str):
    for host_rel, container_path in volumes:
        if container_path.rstrip("/") == container_dir.rstrip("/"):
            return host_rel
    return None


def _read_obs_layout(project_path: str) -> dict:
    """Lee compose-dev.yml (preferido) o compose.yml de la carpeta de obs y
    devuelve, por cada componente presente, su puerto host y las rutas de
    config/provisioning ya resueltas a paths absolutos del host."""
    content = None
    for cf in ("compose-dev.yml", "compose-dev.yaml", "compose.yml", "compose.yaml",
               "docker-compose.yml", "docker-compose.yaml"):
        candidate = os.path.join(project_path, cf)
        if os.path.isfile(candidate):
            try:
                content = open(candidate, encoding="utf-8", errors="ignore").read()
                break
            except Exception:
                continue
    if not content:
        return {}

    container_ports = {"prometheus": 9090, "loki": 3100, "grafana": 3000}
    layout = {}
    for svc_name, svc_body in _parse_compose_services(content).items():
        kind = _classify_obs_service(svc_body)
        if not kind:
            continue
        volumes = _service_volumes(svc_body)
        info = {"volumes": volumes}
        if kind in container_ports:
            info["port"] = _service_host_port(svc_body, container_ports[kind])
        if kind == "prometheus":
            host_rel = _config_mount_host_path(volumes, "/prometheus.yml")
            if host_rel:
                info["config_host"] = os.path.normpath(os.path.join(project_path, host_rel))
        elif kind == "promtail":
            host_rel = _config_mount_host_path(volumes, "/config.yml")
            if host_rel:
                info["config_host"] = os.path.normpath(os.path.join(project_path, host_rel))
        elif kind == "grafana":
            host_rel = _dir_mount_host_path(volumes, "/etc/grafana/provisioning")
            if host_rel:
                info["provisioning_host"] = os.path.normpath(os.path.join(project_path, host_rel))
            m = re.search(r'GF_SECURITY_ADMIN_PASSWORD\s*[:=]\s*["\']?([^"\'\s]+)', svc_body)
            if m:
                info["admin_password"] = m.group(1)
        layout[kind] = info
    return layout


def _read_compose_hints(project_path: str) -> dict:
    """Busca compose-dev.yml (preferido) o compose.yml en la carpeta de infra
    y saca el primer puerto host publicado y cualquier KEYCLOAK_ADMIN* que
    encuentre. Best-effort con regex — no hace falta PyYAML para esto."""
    hints = {"port": None, "admin_user": None, "admin_password": None}
    for cf in ("compose-dev.yml", "compose-dev.yaml",
               "compose.yml", "compose.yaml",
               "docker-compose.yml", "docker-compose.yaml"):
        candidate = os.path.join(project_path, cf)
        if os.path.isfile(candidate):
            try:
                content = open(candidate, encoding="utf-8", errors="ignore").read()
            except Exception:
                continue
            # Preferir el puerto publicado hacia el contenedor 8080 (http de
            # Keycloak de fábrica) — si el compose trae otros servicios antes
            # (ej. su propia BD Postgres), el primer "host:container" del
            # archivo puede no ser el de Keycloak.
            m = re.search(r'["\']?(\d{2,5}):8080["\']?', content)
            if not m:
                m = re.search(r'["\']?(\d{2,5}):\d{2,5}["\']?', content)
            if m:
                hints["port"] = int(m.group(1))
            # KEYCLOAK_ADMIN* (legacy) o KC_BOOTSTRAP_ADMIN_* (Keycloak 26+)
            m = re.search(r'(?:KEYCLOAK_ADMIN|KC_BOOTSTRAP_ADMIN_USERNAME)\s*[:=]\s*["\']?([^"\'\s]+)', content)
            if m:
                hints["admin_user"] = m.group(1)
            m = re.search(r'(?:KEYCLOAK_ADMIN_PASSWORD|KC_BOOTSTRAP_ADMIN_PASSWORD)\s*[:=]\s*["\']?([^"\'\s]+)', content)
            if m:
                hints["admin_password"] = m.group(1)
            break
    return hints


class PortableInfraManager:
    """Arranca servicios de infra (Kafka, Keycloak) como procesos nativos."""

    SUPPORTED = ("kafka", "keycloak", "prometheus", "loki", "promtail", "grafana", "obs")

    def ensure(self, infra_name: str, on_progress=None) -> dict:
        spec = _CATALOG.get(infra_name)
        if not spec:
            return {"ok": False, "error": f"Infra nativa no soportada: {infra_name}"}

        if spec.get("kind") == "flat":
            return self._ensure_flat(infra_name, spec, on_progress)
        if spec.get("kind") == "jar":
            return self._ensure_jar(infra_name, spec, on_progress)

        # No todas las distros traen bin/ (prometheus trae el .exe suelto en
        # la raíz) — chequear bin/ específicamente hacía que prometheus se
        # re-descargara en cada "up", nunca quedaba cacheado. Alcanza con
        # que la carpeta home ya exista y tenga contenido.
        home = os.path.join(_infra_bins_dir(), infra_name, spec["home_name"])
        if os.path.isdir(home) and os.listdir(home):
            return {"ok": True, "home": home, "cached": True}

        base = os.path.join(_infra_bins_dir(), infra_name)
        tmp_root = os.path.join(base, f".tmp-{os.getpid()}")
        shutil.rmtree(tmp_root, ignore_errors=True)
        os.makedirs(tmp_root, exist_ok=True)
        archive = os.path.join(tmp_root, "download" + (".zip" if spec["format"] == "zip" else ".tgz"))
        try:
            _download(spec["url"], archive, on_progress)
            if spec["format"] == "zip":
                _extract_zip(archive, tmp_root)
            else:
                _extract_tgz(archive, tmp_root)
        except Exception as e:
            shutil.rmtree(tmp_root, ignore_errors=True)
            return {"ok": False, "error": f"No se pudo descargar/extraer {infra_name}: {e}"}

        extracted_home = os.path.join(tmp_root, spec["home_name"])
        if not os.path.isdir(extracted_home):
            shutil.rmtree(tmp_root, ignore_errors=True)
            return {"ok": False, "error": f"El archivo de {infra_name} no tenía la carpeta esperada {spec['home_name']}"}

        os.makedirs(base, exist_ok=True)
        os.replace(extracted_home, home)
        shutil.rmtree(tmp_root, ignore_errors=True)
        _grant_user_access(home)
        return {"ok": True, "home": home, "cached": False}

    def _ensure_flat(self, infra_name: str, spec: dict, on_progress=None) -> dict:
        """Loki/Promtail: el zip trae un único .exe suelto, sin carpeta
        contenedora — a diferencia de Kafka/Keycloak/Prometheus/Grafana."""
        home = os.path.join(_infra_bins_dir(), infra_name)
        bin_path = os.path.join(home, spec["bin_name"])
        if os.path.isfile(bin_path):
            return {"ok": True, "home": home, "bin_path": bin_path, "cached": True}

        os.makedirs(home, exist_ok=True)
        tmp_zip = os.path.join(home, f".tmp-{os.getpid()}.zip")
        try:
            _download(spec["url"], tmp_zip, on_progress)
            with zipfile.ZipFile(tmp_zip) as z:
                z.extractall(home)
        except Exception as e:
            return {"ok": False, "error": f"No se pudo descargar/extraer {infra_name}: {e}"}
        finally:
            if os.path.isfile(tmp_zip):
                try:
                    os.remove(tmp_zip)
                except Exception:
                    pass

        if not os.path.isfile(bin_path):
            return {"ok": False, "error": f"{infra_name}: no se encontró el binario {spec['bin_name']} tras extraer"}
        _mark_executable(home)
        _grant_user_access(home)
        return {"ok": True, "home": home, "bin_path": bin_path, "cached": False}

    def _ensure_jar(self, infra_name: str, spec: dict, on_progress=None) -> dict:
        """Kafbat UI: el asset de GitHub ya es el .jar suelto, sin zip que
        descomprimir (a diferencia de los demás)."""
        home = os.path.join(_infra_bins_dir(), infra_name)
        jar_path = os.path.join(home, spec["bin_name"])
        if os.path.isfile(jar_path):
            return {"ok": True, "home": home, "bin_path": jar_path, "cached": True}

        os.makedirs(home, exist_ok=True)
        tmp_path = jar_path + f".tmp-{os.getpid()}"
        try:
            _download(spec["url"], tmp_path, on_progress)
            os.replace(tmp_path, jar_path)
        except Exception as e:
            if os.path.isfile(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            return {"ok": False, "error": f"No se pudo descargar {infra_name}: {e}"}
        _grant_user_access(home)
        return {"ok": True, "home": home, "bin_path": jar_path, "cached": False}

    def _state_path(self, project_path: str) -> str:
        return os.path.join(project_path, ".orq_infra.json")

    def _load_state(self, project_path: str) -> dict:
        p = self._state_path(project_path)
        if os.path.isfile(p):
            try:
                with open(p, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_state(self, project_path: str, state: dict):
        try:
            with open(self._state_path(project_path), "w", encoding="utf-8") as f:
                json.dump(state, f)
        except Exception:
            pass

    def _progress(self, on_log, infra_name):
        if not on_log:
            return None
        state = {"last_pct": -10}

        def _cb(done, total):
            pct = int(done * 100 / total) if total else 0
            if pct >= state["last_pct"] + 10 or pct == 100:
                state["last_pct"] = pct
                mb_done, mb_total = done // (1024 * 1024), total // (1024 * 1024)
                on_log(f"[orquestador] Descargando {infra_name}: {pct}% ({mb_done}MB / {mb_total}MB)")
        return _cb

    def up(self, infra_name: str, project_path: str, on_log=None) -> dict:
        if infra_name not in self.SUPPORTED:
            return {"ok": False,
                    "error": f"'{infra_name}' todavía no tiene arranque nativo "
                             f"(soportados: {', '.join(self.SUPPORTED)})"}

        if infra_name in _OBS_NAMES:
            return self._up_obs(project_path, on_log)
        if infra_name == "kafka":
            return self._up_kafka_bundle(project_path, on_log)

        state = self._load_state(project_path)
        if state.get("pid") and _is_running(state["pid"]) and state.get("port"):
            if _wait_for_port("localhost", state["port"], timeout=2):
                return {"ok": True, "port": state["port"], "already_running": True}

        result = self.ensure(infra_name, on_progress=self._progress(on_log, infra_name))
        if not result.get("ok"):
            return result
        home = result["home"]

        hints = _read_compose_hints(project_path)
        port = hints.get("port") or _CATALOG[infra_name]["default_port"]

        return self._start_keycloak(project_path, home, port, hints, on_log)

    def down(self, infra_name: str, project_path: str) -> dict:
        if infra_name in _OBS_NAMES or infra_name == "kafka":
            return self._down_obs(project_path)
        state = self._load_state(project_path)
        pid = state.get("pid")
        if pid:
            _kill(pid)
        self._save_state(project_path, {})
        return {"ok": True}

    def status(self, project_path: str) -> str:
        state = self._load_state(project_path)
        if "procs" in state:
            return self._status_obs(state)
        pid, port = state.get("pid"), state.get("port")
        if pid and _is_running(pid) and port and _wait_for_port("localhost", port, timeout=1):
            return "RUNNING"
        return "STOPPED"

    # ── Obs (Prometheus + Loki + Promtail + Grafana, bundle completo) ───────

    def _up_obs(self, project_path: str, on_log=None) -> dict:
        state = self._load_state(project_path)
        procs = state.get("procs") or []
        if procs and all(p.get("ok") and _is_running(p["pid"]) and p.get("port")
                          and _wait_for_port("localhost", p["port"], timeout=2) for p in procs):
            return {"ok": True, "procs": procs, "already_running": True}

        layout = _read_obs_layout(project_path)
        if not layout:
            return {"ok": False,
                     "error": "obs: no se encontraron servicios prometheus/loki/promtail/grafana "
                              "en compose.yml/compose-dev.yml"}

        new_procs = []
        loki_port = None

        def _record(name, r):
            if r.get("pid"):
                new_procs.append({"name": name, "pid": r["pid"], "port": r.get("port"), "ok": r.get("ok", False)})
            return r

        if "loki" in layout:
            r = _record("loki", self._start_loki(project_path, layout["loki"], on_log))
            if r.get("ok"):
                loki_port = r.get("port")
        if "prometheus" in layout:
            _record("prometheus", self._start_prometheus(project_path, layout["prometheus"], on_log))
        if "promtail" in layout:
            _record("promtail", self._start_promtail(project_path, layout["promtail"], loki_port, on_log))
        if "grafana" in layout:
            prom_port = next((p["port"] for p in new_procs if p["name"] == "prometheus" and p["ok"]), None)
            _record("grafana", self._start_grafana(project_path, layout["grafana"], prom_port, loki_port, on_log))

        self._save_state(project_path, {"infra_name": "obs", "procs": new_procs})

        ok_names = [p["name"] for p in new_procs if p["ok"]]
        bad_names = [p["name"] for p in new_procs if not p["ok"]]
        if not ok_names:
            return {"ok": False, "error": f"obs: ningún componente arrancó ({', '.join(bad_names) or 'sin servicios detectados'})"}

        main_port = next((p["port"] for p in new_procs if p["name"] == "grafana" and p["ok"]),
                          new_procs[0].get("port"))
        result = {"ok": True, "port": main_port, "procs": new_procs}
        if bad_names:
            result["warning"] = f"obs: no arrancaron: {', '.join(bad_names)}"
            if on_log:
                on_log(f"[orquestador] obs: componentes con error: {', '.join(bad_names)}")
        return result

    def _down_obs(self, project_path: str) -> dict:
        state = self._load_state(project_path)
        for p in state.get("procs", []):
            _kill(p.get("pid"))
        self._save_state(project_path, {})
        return {"ok": True}

    def _status_obs(self, state: dict) -> str:
        procs = state.get("procs", [])
        if not procs:
            return "STOPPED"
        alive = [p for p in procs if p.get("ok") and _is_running(p["pid"]) and p.get("port")
                 and _wait_for_port("localhost", p["port"], timeout=1)]
        return "RUNNING" if alive else "STOPPED"

    def _start_prometheus(self, project_path: str, info: dict, on_log) -> dict:
        result = self.ensure("prometheus", on_progress=self._progress(on_log, "prometheus"))
        if not result.get("ok"):
            return result
        home = result["home"]
        data_dir = _infra_data_dir("obs_prometheus", project_path)
        port = info.get("port") or _CATALOG["prometheus"]["default_port"]

        text = ""
        cfg_src = info.get("config_host")
        if cfg_src and os.path.isfile(cfg_src):
            try:
                text = open(cfg_src, encoding="utf-8", errors="replace").read()
            except Exception:
                text = ""
        if not text.strip():
            text = "global:\n  scrape_interval: 15s\nscrape_configs: []\n"
        # En Docker, para llegar al host desde el contenedor se usa
        # host.docker.internal; en modo nativo Prometheus ya corre en el
        # mismo host, así que basta con localhost.
        text = text.replace("host.docker.internal", "localhost")
        cfg_path = os.path.join(data_dir, "prometheus.yml")
        with open(cfg_path, "w", encoding="utf-8") as f:
            f.write(text)

        ext = ".exe" if _IS_WINDOWS else ""
        bin_path = os.path.join(home, f"prometheus{ext}")
        log_file = open(os.path.join(data_dir, "prometheus.log"), "ab")
        creationflags = subprocess.CREATE_NO_WINDOW if _IS_WINDOWS else 0
        proc = subprocess.Popen(
            [bin_path, f"--config.file={cfg_path}", f"--web.listen-address=:{port}",
             f"--storage.tsdb.path={os.path.join(data_dir, 'tsdb')}"],
            stdout=log_file, stderr=subprocess.STDOUT, cwd=home, creationflags=creationflags,
        )
        if on_log:
            on_log(f"[orquestador] Prometheus arrancando en localhost:{port} ...")
        ok = _wait_for_port("localhost", port, timeout=30)
        return {"ok": ok, "port": port, "pid": proc.pid,
                "error": None if ok else f"Prometheus no respondió en el puerto {port} tras 30s"}

    def _start_loki(self, project_path: str, info: dict, on_log) -> dict:
        result = self.ensure("loki", on_progress=self._progress(on_log, "loki"))
        if not result.get("ok"):
            return result
        bin_path = result["bin_path"]
        data_dir = _infra_data_dir("obs_loki", project_path)
        port = info.get("port") or _CATALOG["loki"]["default_port"]
        grpc_port = port + 1
        posix_data_dir = data_dir.replace(os.sep, "/")

        cfg = f"""auth_enabled: false

server:
  http_listen_port: {port}
  grpc_listen_port: {grpc_port}

common:
  path_prefix: {posix_data_dir}
  storage:
    filesystem:
      chunks_directory: {posix_data_dir}/chunks
      rules_directory: {posix_data_dir}/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: 2024-01-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

limits_config:
  allow_structured_metadata: false
"""
        cfg_path = os.path.join(data_dir, "loki-config.yaml")
        with open(cfg_path, "w", encoding="utf-8") as f:
            f.write(cfg)

        log_file = open(os.path.join(data_dir, "loki.log"), "ab")
        creationflags = subprocess.CREATE_NO_WINDOW if _IS_WINDOWS else 0
        proc = subprocess.Popen(
            [bin_path, f"-config.file={cfg_path}"],
            stdout=log_file, stderr=subprocess.STDOUT, cwd=data_dir, creationflags=creationflags,
        )
        if on_log:
            on_log(f"[orquestador] Loki arrancando en localhost:{port} ...")
        ok = _wait_for_port("localhost", port, timeout=30)
        return {"ok": ok, "port": port, "pid": proc.pid,
                "error": None if ok else f"Loki no respondió en el puerto {port} tras 30s"}

    def _start_promtail(self, project_path: str, info: dict, loki_port, on_log) -> dict:
        result = self.ensure("promtail", on_progress=self._progress(on_log, "promtail"))
        if not result.get("ok"):
            return result
        bin_path = result["bin_path"]
        data_dir = _infra_data_dir("obs_promtail", project_path)

        text = None
        cfg_src = info.get("config_host")
        if cfg_src and os.path.isfile(cfg_src):
            try:
                text = open(cfg_src, encoding="utf-8", errors="replace").read()
            except Exception:
                text = None

        port = _CATALOG["promtail"]["default_port"]
        effective_loki_port = loki_port or _CATALOG["loki"]["default_port"]

        if text:
            m = re.search(r'http_listen_port:\s*(\d+)', text)
            if m:
                port = int(m.group(1))
            text = re.sub(r'url:\s*http://[^/\s]+(/loki/api/v1/push)',
                           f'url: http://localhost:{effective_loki_port}\\1', text)
            positions_path = os.path.join(data_dir, "positions.yaml").replace(os.sep, "/")
            text = re.sub(r'(positions:\s*\n\s*filename:\s*)\S+', lambda m: m.group(1) + positions_path, text)
            for host_rel, container_path in sorted(info.get("volumes", []), key=lambda hv: -len(hv[1])):
                host_abs = os.path.normpath(os.path.join(project_path, host_rel)).replace(os.sep, "/")
                text = text.replace(container_path, host_abs)
        else:
            posix_positions = os.path.join(data_dir, "positions.yaml").replace(os.sep, "/")
            text = f"""server:
  http_listen_port: {port}

positions:
  filename: {posix_positions}

clients:
  - url: http://localhost:{effective_loki_port}/loki/api/v1/push

scrape_configs: []
"""
        cfg_path = os.path.join(data_dir, "promtail-config.yaml")
        with open(cfg_path, "w", encoding="utf-8") as f:
            f.write(text)

        log_file = open(os.path.join(data_dir, "promtail.log"), "ab")
        creationflags = subprocess.CREATE_NO_WINDOW if _IS_WINDOWS else 0
        proc = subprocess.Popen(
            [bin_path, f"-config.file={cfg_path}"],
            stdout=log_file, stderr=subprocess.STDOUT, cwd=data_dir, creationflags=creationflags,
        )
        if on_log:
            on_log(f"[orquestador] Promtail arrancando en localhost:{port} (lee logs de disco) ...")
        ok = _wait_for_port("localhost", port, timeout=20)
        return {"ok": ok, "port": port, "pid": proc.pid,
                "error": None if ok else f"Promtail no respondió en el puerto {port} tras 20s"}

    def _start_grafana(self, project_path: str, info: dict, prom_port, loki_port, on_log) -> dict:
        result = self.ensure("grafana", on_progress=self._progress(on_log, "grafana"))
        if not result.get("ok"):
            return result
        home = result["home"]
        data_dir = _infra_data_dir("obs_grafana", project_path)
        logs_dir = os.path.join(data_dir, "logs")
        grafana_data_dir = os.path.join(data_dir, "grafana-data")
        prov_dst = os.path.join(data_dir, "provisioning")
        os.makedirs(logs_dir, exist_ok=True)
        os.makedirs(grafana_data_dir, exist_ok=True)

        prov_src = info.get("provisioning_host")
        shutil.rmtree(prov_dst, ignore_errors=True)
        if prov_src and os.path.isdir(prov_src):
            shutil.copytree(prov_src, prov_dst)
            ds_dir = os.path.join(prov_dst, "datasources")
            if os.path.isdir(ds_dir):
                for fname in os.listdir(ds_dir):
                    fpath = os.path.join(ds_dir, fname)
                    if not os.path.isfile(fpath):
                        continue
                    try:
                        text = open(fpath, encoding="utf-8", errors="replace").read()
                    except Exception:
                        continue
                    # Docker usa nombres DNS de servicio (lamb-prometheus,
                    # lamb-loki); en nativo todo corre en localhost.
                    if prom_port:
                        text = re.sub(r'http://[^/\s]+:9090', f'http://localhost:{prom_port}', text)
                    if loki_port:
                        text = re.sub(r'http://[^/\s]+:3100', f'http://localhost:{loki_port}', text)
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(text)
        else:
            os.makedirs(prov_dst, exist_ok=True)

        port = info.get("port") or _CATALOG["grafana"]["default_port"]
        admin_pass = info.get("admin_password") or "admin"

        env = os.environ.copy()
        env["GF_SECURITY_ADMIN_PASSWORD"] = admin_pass
        env["GF_PATHS_DATA"] = grafana_data_dir
        env["GF_PATHS_LOGS"] = logs_dir
        env["GF_PATHS_PROVISIONING"] = prov_dst
        env["GF_SERVER_HTTP_PORT"] = str(port)

        ext = ".exe" if _IS_WINDOWS else ""
        bin_path = os.path.join(home, "bin", f"grafana-server{ext}")

        log_file = open(os.path.join(data_dir, "grafana.log"), "ab")
        creationflags = subprocess.CREATE_NO_WINDOW if _IS_WINDOWS else 0
        proc = subprocess.Popen(
            [bin_path, "--homepath", home],
            stdout=log_file, stderr=subprocess.STDOUT, cwd=home, env=env, creationflags=creationflags,
        )
        if on_log:
            on_log(f"[orquestador] Grafana arrancando en localhost:{port} (admin/{admin_pass}) ...")
        ok = _wait_for_port("localhost", port, timeout=45)
        return {"ok": ok, "port": port, "pid": proc.pid,
                "error": None if ok else f"Grafana no respondió en el puerto {port} tras 45s"}

    # ── Kafka (KRaft, single-node) ──────────────────────────────────────────

    def _up_kafka_bundle(self, project_path: str, on_log=None) -> dict:
        """Kafka (broker nativo) + Kafbat UI (admin web). El app_url que
        termina viendo el usuario es el de Kafbat, no el puerto crudo del
        broker — Kafka habla protocolo binario, no hay nada que abrir ahí."""
        state = self._load_state(project_path)
        procs = state.get("procs") or []
        if procs and all(p.get("ok") and _is_running(p["pid"]) and p.get("port")
                          and _wait_for_port("localhost", p["port"], timeout=2) for p in procs):
            return {"ok": True, "port": self._kafka_bundle_app_port(procs), "procs": procs, "already_running": True}

        result = self.ensure("kafka", on_progress=self._progress(on_log, "kafka"))
        if not result.get("ok"):
            return result
        home = result["home"]

        hints = _read_compose_hints(project_path)
        kafka_port = hints.get("port") or _CATALOG["kafka"]["default_port"]

        new_procs = []

        def _record(name, r):
            if r.get("pid"):
                new_procs.append({"name": name, "pid": r["pid"], "port": r.get("port"), "ok": r.get("ok", False)})
            return r

        kafka_res = _record("kafka", self._start_kafka(project_path, home, kafka_port, on_log))
        if not kafka_res.get("ok"):
            self._save_state(project_path, {"infra_name": "kafka", "procs": new_procs})
            return {"ok": False, "error": kafka_res.get("error", "Kafka no arrancó")}

        kafbat_res = _record("kafbat", self._start_kafbat(project_path, kafka_port, on_log))
        self._save_state(project_path, {"infra_name": "kafka", "procs": new_procs})

        result = {"ok": True, "port": self._kafka_bundle_app_port(new_procs), "procs": new_procs}
        if not kafbat_res.get("ok"):
            result["warning"] = f"Kafka arrancó pero Kafbat UI no: {kafbat_res.get('error')}"
            if on_log:
                on_log(f"[orquestador] Kafbat UI no arrancó: {kafbat_res.get('error')}")
        return result

    def _kafka_bundle_app_port(self, procs: list):
        kafbat = next((p for p in procs if p.get("name") == "kafbat" and p.get("ok")), None)
        if kafbat:
            return kafbat.get("port")
        kafka = next((p for p in procs if p.get("name") == "kafka"), None)
        return kafka.get("port") if kafka else None

    def _start_kafbat(self, project_path: str, kafka_port: int, on_log) -> dict:
        if not shutil.which("java"):
            return {"ok": False, "error": "Kafbat UI necesita Java (JDK 17+) instalado y en el PATH"}

        result = self.ensure("kafbat", on_progress=self._progress(on_log, "kafbat"))
        if not result.get("ok"):
            return result
        jar_path = result["bin_path"]
        port = _CATALOG["kafbat"]["default_port"]

        env = os.environ.copy()
        env["SERVER_PORT"] = str(port)
        env["KAFKA_CLUSTERS_0_NAME"] = "local"
        env["KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS"] = f"localhost:{kafka_port}"

        log_file = open(os.path.join(_infra_data_dir("kafbat", project_path), "kafbat.log"), "ab")
        creationflags = subprocess.CREATE_NO_WINDOW if _IS_WINDOWS else 0
        proc = subprocess.Popen(
            ["java", "-jar", jar_path],
            stdout=log_file, stderr=subprocess.STDOUT,
            cwd=os.path.dirname(jar_path), env=env, creationflags=creationflags,
        )
        if on_log:
            on_log(f"[orquestador] Kafbat UI arrancando en localhost:{port} ...")
        ok = _wait_for_port("localhost", port, timeout=45)
        return {"ok": ok, "port": port, "pid": proc.pid,
                "error": None if ok else f"Kafbat UI no respondió en el puerto {port} tras 45s"}

    def _start_kafka(self, project_path: str, home: str, port: int, on_log) -> dict:
        data_dir = _infra_data_dir("kafka", project_path)
        log_dir = os.path.join(data_dir, "kraft-logs")
        config_dir = os.path.join(data_dir, "config")
        os.makedirs(config_dir, exist_ok=True)
        controller_port = port + 1000

        server_properties = os.path.join(config_dir, "server.properties")
        node_id = 1
        cluster_id = "MkU3OEVBNTcwNTJENDM2Qk"
        props = f"""process.roles=broker,controller
node.id={node_id}
controller.quorum.voters={node_id}@localhost:{controller_port}
listeners=PLAINTEXT://localhost:{port},CONTROLLER://localhost:{controller_port}
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:{port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={log_dir.replace(os.sep, '/')}
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
"""
        with open(server_properties, "w", encoding="utf-8") as f:
            f.write(props)

        # En Windows, los .bat de Kafka arman el CLASSPATH concatenando cada
        # jar de libs/ uno por uno con "set" — con los ~100+ jars de Kafka
        # 4.x esa línea supera el límite de 8191 caracteres de cmd.exe y
        # revienta con "La línea escrita es demasiado larga". Invocamos java
        # directo con classpath wildcard (libs\*, que java expande nativo sin
        # pasar por cmd) para esquivar el bug; en Linux/Mac seguimos usando
        # los scripts .sh de siempre.
        log4j_cfg = os.path.join(home, "config", "tools-log4j2.yaml")
        server_log4j_cfg = os.path.join(home, "config", "log4j2.yaml")

        meta_file = os.path.join(log_dir, "meta.properties")
        if not os.path.isfile(meta_file):
            if _IS_WINDOWS:
                classpath = os.path.join(home, "libs", "*")
                fmt_cmd = [
                    "java", "-Dlog4j2.configurationFile=file:" + log4j_cfg,
                    "-cp", classpath, "kafka.tools.StorageTool",
                    "format", "-t", cluster_id, "-c", server_properties, "--ignore-formatted",
                ]
            else:
                storage_cmd = os.path.join(home, "bin", "kafka-storage.sh")
                fmt_cmd = [storage_cmd, "format", "-t", cluster_id, "-c", server_properties, "--ignore-formatted"]
            fmt = subprocess.run(fmt_cmd, capture_output=True, text=True, timeout=60)
            if fmt.returncode != 0:
                return {"ok": False, "error": f"kafka-storage format falló: {fmt.stderr or fmt.stdout}"}

        log_file = open(os.path.join(data_dir, "kafka.log"), "ab")
        creationflags = subprocess.CREATE_NO_WINDOW if _IS_WINDOWS else 0
        if _IS_WINDOWS:
            classpath = os.path.join(home, "libs", "*")
            server_cmd_list = [
                "java", "-Xmx1G", "-Xms1G",
                "-Dlog4j2.configurationFile=file:" + server_log4j_cfg,
                "-cp", classpath, "kafka.Kafka", server_properties,
            ]
        else:
            server_cmd_list = [os.path.join(home, "bin", "kafka-server-start.sh"), server_properties]
        proc = subprocess.Popen(
            server_cmd_list,
            stdout=log_file, stderr=subprocess.STDOUT,
            cwd=home, creationflags=creationflags,
        )
        self._save_state(project_path, {"infra_name": "kafka", "pid": proc.pid, "port": port})

        if on_log:
            on_log(f"[orquestador] Kafka (KRaft) arrancando en localhost:{port} ...")
        if not _wait_for_port("localhost", port, timeout=60):
            return {"ok": False, "error": f"Kafka no respondió en el puerto {port} tras 60s"}
        return {"ok": True, "port": port}

    # ── Keycloak ─────────────────────────────────────────────────────────────

    def _start_keycloak(self, project_path: str, home: str, port: int, hints: dict, on_log) -> dict:
        import_src = os.path.join(project_path, "import")
        import_dst = os.path.join(home, "data", "import")
        if os.path.isdir(import_src):
            os.makedirs(import_dst, exist_ok=True)
            for fname in os.listdir(import_src):
                if fname.lower().endswith(".json"):
                    shutil.copyfile(os.path.join(import_src, fname), os.path.join(import_dst, fname))

        admin_user = hints.get("admin_user") or "admin"
        admin_pass = hints.get("admin_password") or "admin"

        ext = ".bat" if _IS_WINDOWS else ".sh"
        kc_cmd = os.path.join(home, "bin", f"kc{ext}")

        env = os.environ.copy()
        env["KEYCLOAK_ADMIN"] = admin_user
        env["KEYCLOAK_ADMIN_PASSWORD"] = admin_pass

        log_file = open(os.path.join(_infra_data_dir("keycloak", project_path), "keycloak.log"), "ab")
        creationflags = subprocess.CREATE_NO_WINDOW if _IS_WINDOWS else 0
        proc = subprocess.Popen(
            [kc_cmd, "start-dev", f"--http-port={port}", "--import-realm"],
            stdout=log_file, stderr=subprocess.STDOUT,
            cwd=home, env=env, creationflags=creationflags,
        )
        self._save_state(project_path, {"infra_name": "keycloak", "pid": proc.pid, "port": port})

        if on_log:
            on_log(f"[orquestador] Keycloak arrancando en localhost:{port} (admin/{admin_user}) ...")
        if not _wait_for_port("localhost", port, timeout=90):
            return {"ok": False, "error": f"Keycloak no respondió en el puerto {port} tras 90s"}
        return {"ok": True, "port": port}

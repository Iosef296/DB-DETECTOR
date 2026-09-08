import shutil
import subprocess

from core.docker_manager import DockerManager
from core.portable_manager import PortableManager


def _is_docker_running() -> bool:
    # Primero verificar que el binario 'docker' existe en el PATH.
    # Hacemos esto antes de ejecutar cualquier subprocess porque en sistemas sin Docker
    # instalado, intentar correr "docker info" lanzaría FileNotFoundError, no un returncode.
    if not shutil.which("docker"):
        return False
    try:
        # Usamos "docker info" en lugar de "docker ps" porque "docker info" falla
        # específicamente cuando el daemon no está corriendo (returncode != 0),
        # mientras que "docker ps" puede fallar por permisos aunque el daemon esté vivo.
        # timeout=3 porque si el daemon está colgado puede tardar indefinidamente;
        # 3s es suficiente para una respuesta local y no bloquea el arranque del Gestor.
        r = subprocess.run(["docker", "info"], capture_output=True, timeout=3)
        return r.returncode == 0
    except Exception:
        # TimeoutExpired, PermissionError, etc. → tratar como "no disponible"
        return False


def get_manager() -> "DockerManager | PortableManager":
    # Decisión de arquitectura: la selección ocurre UNA vez en la construcción del Gestor.
    # No se re-evalúa en cada operación porque:
    #   1. Si Docker muere a mitad de una sesión, no queremos cambiar de manager
    #      de forma silenciosa y corromper el estado de los proyectos.
    #   2. _is_docker_running() corre un subprocess → costoso si se llama en cada request.
    # El Gestor asume que el manager elegido al inicio es el correcto para toda la sesión.
    if _is_docker_running():
        return DockerManager()
    # PortableManager hereda de NativeManager y agrega descarga automática de binarios.
    # Es el fallback final: funciona sin Docker Y sin binarios instalados en el sistema.
    return PortableManager()


def get_manager_name() -> str:
    # Útil para mostrar en la UI qué modo está activo.
    # Llama a _is_docker_running() de nuevo (no cachea) porque get_manager_name()
    # se usa solo en endpoints de estado, no en el hot path de operaciones.
    return "docker" if _is_docker_running() else "portable"

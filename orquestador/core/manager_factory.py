from core.portable_manager import PortableManager


def get_manager() -> "PortableManager":
    # Decisión de arquitectura: siempre nativo/portable, sin depender de Docker.
    # Los binarios de BD (Postgres, MySQL, Mongo, Redis) se descargan una sola
    # vez a una carpeta propia (~/.orquestador/bins) y corren como procesos
    # nativos — nada se instala a nivel de sistema, y no hace falta tener
    # Docker instalado ni corriendo.
    return PortableManager()


def get_manager_name() -> str:
    return "portable"

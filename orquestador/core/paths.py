"""
Rutas de datos persistentes del orquestador.

Todo lo que debe sobrevivir a un reinicio (SQLite del registro, binarios de BD
descargados, repos clonados, datos de cada BD) vive bajo un único directorio raíz.

- Local (default): ~/.orquestador
- Railway / contenedor: setear ORQ_DATA_DIR=/data (volumen montado)

Se centraliza aquí para que native_manager, portable_manager y api/server.py
usen exactamente la misma base sin duplicar la lógica del env var.
"""

import os


def data_root() -> str:
    """Directorio raíz para todos los datos persistentes. Se crea si no existe."""
    root = os.environ.get("ORQ_DATA_DIR", "").strip()
    if not root:
        root = os.path.expanduser("~/.orquestador")
    os.makedirs(root, exist_ok=True)
    return root


def data_path(*parts: str) -> str:
    """Ruta bajo data_root(), creando los directorios intermedios."""
    full = os.path.join(data_root(), *parts)
    os.makedirs(os.path.dirname(full) or full, exist_ok=True)
    return full


def projects_dir() -> str:
    """Directorio donde se clonan/extraen los proyectos registrados por Git o ZIP."""
    p = os.path.join(data_root(), "projects")
    os.makedirs(p, exist_ok=True)
    return p

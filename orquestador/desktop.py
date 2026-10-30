"""Punto de entrada de la app desktop.

Arranca el mismo Flask de siempre (api/server.py) en un hilo y lo muestra en
una ventana nativa de Windows (WebView2) en vez de abrir el navegador. Así:
  - no hay consola que cerrar sin querer y matar el server con ella
  - no hay ventanas de PowerShell/UAC sueltas: el UAC salta UNA vez al abrir
    la app (si hace falta admin), después todo vive en esta sola ventana
  - "Liberar RAM" puede vaciar el standby list del sistema porque la app
    corre elevada de punta a punta
"""
import os
import sys
import ctypes
import platform
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

_IS_WINDOWS = platform.system() == "Windows"
_PORT = 7433


def _is_admin() -> bool:
    if not _IS_WINDOWS:
        return hasattr(os, "geteuid") and os.geteuid() == 0
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _relaunch_as_admin():
    # ShellExecuteW con verbo "runas" dispara el UAC una sola vez al abrir la
    # app — a partir de ahí todo (server + ventana) corre ya elevado, sin
    # pedir de nuevo ni abrir ventanas extra.
    params = " ".join(f'"{a}"' for a in sys.argv)
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)


def main():
    if _IS_WINDOWS and not _is_admin():
        _relaunch_as_admin()
        return

    import webview
    from api.server import app, _ensure_single_instance, set_desktop_window

    _ensure_single_instance(_PORT)

    threading.Thread(
        target=lambda: app.run(host="127.0.0.1", port=_PORT, debug=False, use_reloader=False),
        daemon=True,
    ).start()

    window = webview.create_window(
        "DB-Detector — Orquestador",
        f"http://localhost:{_PORT}",
        width=1360, height=840, min_size=(960, 600),
    )
    set_desktop_window(window)
    webview.start()


if __name__ == "__main__":
    main()

import sys
import subprocess
from pathlib import Path

_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))

DEPS = [
    "flask",
    "click",
    "rich",
    "psycopg2-binary",
    "pymysql",
    "pymongo",
    "redis",
]


def install_deps_if_needed():
    import importlib
    missing = []
    _name_map = {
        "psycopg2-binary": "psycopg2",
        "pymysql":         "pymysql",
        "pymongo":         "pymongo",
        "redis":           "redis",
        "flask":           "flask",
        "click":           "click",
        "rich":            "rich",
    }
    for pkg in DEPS:
        mod = _name_map.get(pkg, pkg)
        try:
            importlib.import_module(mod)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"Instalando dependencias: {', '.join(missing)}")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing,
            check=True
        )


if __name__ == "__main__":
    # Strip our own script name so Click/Flask don't get confused
    if len(sys.argv) > 1 and sys.argv[1] not in ("--no-browser",):
        install_deps_if_needed()
        from cli.main import cli
        # Re-invoke with remaining args
        sys.argv = [sys.argv[0]] + sys.argv[1:]
        cli()
    else:
        install_deps_if_needed()
        from api.server import app, main
        main()

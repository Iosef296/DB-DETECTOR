#!/usr/bin/env python3
"""
DB Detector - Script de instalación y arranque
Uso: python run.py
"""
import subprocess
import sys
import os

REQUIRED = ["flask", "psycopg2-binary", "pymysql", "pymongo", "redis"]

def install_deps():
    print("📦 Instalando dependencias...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet"] + REQUIRED)
    print("✅ Dependencias instaladas\n")

def main():
    try:
        import flask
    except ImportError:
        install_deps()

    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Importar y arrancar el servidor
    from server import main as run_server
    run_server()

if __name__ == "__main__":
    main()

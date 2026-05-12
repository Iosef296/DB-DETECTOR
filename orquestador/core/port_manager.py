import sqlite3
import socket


DB_PORT_RANGE  = range(5500, 5601)
APP_PORT_RANGE = range(8100, 8201)


class PortManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_used(self) -> tuple[set, set]:
        try:
            con = sqlite3.connect(self.db_path)
            rows = con.execute("SELECT db_port, app_port FROM projects").fetchall()
            con.close()
        except Exception:
            rows = []
        db_ports  = {r[0] for r in rows if r[0]}
        app_ports = {r[1] for r in rows if r[1]}
        return db_ports, app_ports

    def is_port_free(self, port: int) -> bool:
        try:
            with socket.create_connection(("localhost", port), timeout=0.5):
                return False
        except ConnectionRefusedError:
            return True
        except OSError:
            return True

    def assign_ports(self, project_name: str) -> tuple[int, int]:
        used_db, used_app = self._get_used()

        db_port = None
        for p in DB_PORT_RANGE:
            if p not in used_db and self.is_port_free(p):
                db_port = p
                break
        if db_port is None:
            raise ValueError(
                f"No hay puertos de BD disponibles en rango {DB_PORT_RANGE.start}-{DB_PORT_RANGE.stop - 1}"
            )

        app_port = None
        for p in APP_PORT_RANGE:
            if p not in used_app and self.is_port_free(p):
                app_port = p
                break
        if app_port is None:
            raise ValueError(
                f"No hay puertos de App disponibles en rango {APP_PORT_RANGE.start}-{APP_PORT_RANGE.stop - 1}"
            )

        return db_port, app_port

    def release_ports(self, project_name: str):
        # Ports are freed by deleting the row from SQLite
        pass

    def get_used_ports(self) -> dict:
        used_db, used_app = self._get_used()
        return {"db_ports": sorted(used_db), "app_ports": sorted(used_app)}

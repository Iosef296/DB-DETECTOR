import json
import importlib
from typing import Any, Optional

# ── Hints de instalación ───────────────────────────────────────────────────────
# Si el driver Python no está instalado, le decimos al usuario qué hacer

_DRIVER_HINTS = {
    "postgresql": {"pip": "psycopg2-binary"},
    "mysql":      {"pip": "pymysql"},
    "mongodb":    {"pip": "pymongo"},
    "redis":      {"pip": "redis"},
}

# Si el servidor de BD no está levantado, ofrecemos comandos para instalarlo/arrancarlo
_SERVER_HINTS = {
    "postgresql": {
        "docker": "docker run -d --name postgres -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:16",
        "apt":    "sudo apt-get install -y postgresql && sudo systemctl start postgresql",
        "brew":   "brew install postgresql && brew services start postgresql",
    },
    "mysql": {
        "docker": "docker run -d --name mysql -e MYSQL_ROOT_PASSWORD=pass -p 3306:3306 mysql:8",
        "apt":    "sudo apt-get install -y mysql-server && sudo systemctl start mysql",
        "brew":   "brew install mysql && brew services start mysql",
    },
    "mongodb": {
        "docker": "docker run -d --name mongo -p 27017:27017 mongo:7",
        "apt":    "sudo apt-get install -y mongodb && sudo systemctl start mongodb",
        "brew":   "brew tap mongodb/brew && brew install mongodb-community && brew services start mongodb-community",
    },
    "redis": {
        "docker": "docker run -d --name redis -p 6379:6379 redis:7",
        "apt":    "sudo apt-get install -y redis-server && sudo systemctl start redis-server",
        "brew":   "brew install redis && brew services start redis",
    },
}

def _is_server_down(err: str) -> bool:
    """Detecta si el error es de conexión rechazada (servidor apagado, no credenciales malas)."""
    keywords = ["connection refused", "connect etimedout", "no route to host",
                 "errno 111", "errno 61", "timeout", "timed out", "unreachable",
                 "serverselectiontimeouterror", "could not connect"]
    low = err.lower()
    return any(k in low for k in keywords)

# ── Conexión dinámica ─────────────────────────────────────────────────────────

class DBConnection:
    def __init__(self, credentials: dict):
        # credentials: dict con "type", "host", "port", "user", "password", "database"
        self.creds = credentials
        self.db_type = credentials.get("type")
        self._conn   = None    # conexión activa al driver
        self._cursor = None    # cursor SQL (no aplica para MongoDB/Redis)

    def connect(self) -> dict:
        """Intenta conectar a la BD según su tipo. Enriquece el error con hints si falla."""
        try:
            # Despacho por tipo de BD
            if self.db_type == "postgresql":
                result = self._connect_postgres()
            elif self.db_type == "mysql":
                result = self._connect_mysql()
            elif self.db_type == "mongodb":
                result = self._connect_mongo()
            elif self.db_type == "sqlite":
                result = self._connect_sqlite()
            elif self.db_type == "redis":
                result = self._connect_redis()
            else:
                return {"ok": False, "error": f"Tipo de BD no soportado: {self.db_type}"}

            # Si falló, adjuntar sugerencia de instalación para guiar al usuario
            if not result.get("ok"):
                self._attach_install_hint(result)
            return result
        except Exception as e:
            result = {"ok": False, "error": str(e)}
            self._attach_install_hint(result)
            return result

    def _attach_install_hint(self, result: dict):
        """Añade install_hint al resultado según el tipo de error."""
        err = result.get("error", "")
        db  = self.db_type
        # Error de driver Python faltante (ModuleNotFoundError)
        if "no instalado" in err or "No module named" in err:
            hint = _DRIVER_HINTS.get(db, {}).copy()
            hint["type"] = "driver"
            result["install_hint"] = hint
        # Error de servidor no accesible (connection refused, timeout...)
        elif _is_server_down(err):
            hint = _SERVER_HINTS.get(db, {}).copy()
            hint["type"] = "server"
            hint["db"]   = db
            result["install_hint"] = hint

    def _connect_postgres(self):
        # Importar psycopg2 dinámicamente para no requerir la instalación si no se usa
        try:
            psycopg2 = importlib.import_module("psycopg2")
        except ImportError:
            return {"ok": False, "error": "psycopg2 no instalado. Ejecuta: pip install psycopg2-binary"}
        c = self.creds
        self._conn = psycopg2.connect(
            host=c.get("host", "localhost"),
            port=int(c.get("port", 5432)),
            user=c.get("user", ""),
            password=c.get("password", ""),
            database=c.get("database", ""),
            connect_timeout=5,
        )
        self._conn.autocommit = True  # sin autocommit cada query necesitaría commit()
        self._cursor = self._conn.cursor()
        return {"ok": True}

    def _connect_mysql(self):
        try:
            pymysql = importlib.import_module("pymysql")
        except ImportError:
            return {"ok": False, "error": "pymysql no instalado. Ejecuta: pip install pymysql"}
        c = self.creds
        db = c.get("database") or None  # None en vez de "" para que MySQL conecte sin BD
        self._conn = pymysql.connect(
            host=c.get("host", "localhost"),
            port=int(c.get("port", 3306)),
            user=c.get("user", ""),
            password=c.get("password", ""),
            database=db,
            connect_timeout=5,
            autocommit=True,
        )
        self._cursor = self._conn.cursor()
        return {"ok": True}

    def _connect_mongo(self):
        try:
            pymongo = importlib.import_module("pymongo")
        except ImportError:
            return {"ok": False, "error": "pymongo no instalado. Ejecuta: pip install pymongo"}
        c = self.creds
        host = c.get("host", "localhost")
        port = int(c.get("port", 27017))
        # raw_url incluye credenciales y authSource=admin cuando hay auth
        url = c.get("raw_url") or f"mongodb://{host}:{port}/"
        client = pymongo.MongoClient(url, serverSelectionTimeoutMS=5000)
        client.server_info()  # forzar la conexión real (lazy por defecto en pymongo)
        self._conn = client
        return {"ok": True}

    def _connect_sqlite(self):
        import sqlite3
        # SQLite no necesita servidor → el archivo es la BD
        path = self.creds.get("path", ":memory:")
        self._conn = sqlite3.connect(path)
        self._cursor = self._conn.cursor()
        return {"ok": True}

    def _connect_redis(self):
        try:
            redis = importlib.import_module("redis")
        except ImportError:
            return {"ok": False, "error": "redis no instalado. Ejecuta: pip install redis"}
        c = self.creds
        self._conn = redis.Redis(
            host=c.get("host", "localhost"),
            port=int(c.get("port", 6379)),
            username=c.get("user") or None,
            password=c.get("password") or None,
            db=int(c.get("database") or 0),
            socket_connect_timeout=5,
            decode_responses=True,  # devolver strings en vez de bytes
        )
        self._conn.ping()  # probar la conexión
        return {"ok": True}

    def _mongo_db(self):
        """Devuelve el objeto db de Mongo, con fallback a la primera BD no-sistema."""
        db_name = self.creds.get("database", "").strip()
        if db_name:
            return self._conn[db_name]
        # Intentar obtener la BD por defecto embebida en la URL
        try:
            return self._conn.get_default_database()
        except Exception:
            pass
        # Fallback: primera BD que no sea admin/local/config
        system_dbs = {"admin", "local", "config"}
        for name in self._conn.list_database_names():
            if name not in system_dbs:
                return self._conn[name]
        return self._conn["admin"]

    def disconnect(self):
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass

    # ── Operaciones ───────────────────────────────────────────────────────────

    def list_tables(self) -> dict:
        """Lista tablas/colecciones de la BD activa con su tamaño."""
        try:
            if self.db_type == "postgresql":
                # pg_total_relation_size incluye índices y TOAST (tamaño real en disco)
                self._cursor.execute("""
                    SELECT tablename AS table_name,
                           pg_size_pretty(COALESCE(
                               pg_total_relation_size(to_regclass(schemaname || '.' || tablename)), 0
                           )) AS size
                    FROM pg_tables
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY schemaname, tablename
                """)
                rows = self._cursor.fetchall()
                return {"ok": True, "tables": [{"name": r[0], "size": r[1]} for r in rows]}

            elif self.db_type == "mysql":
                # data_length + index_length = tamaño total de la tabla
                self._cursor.execute("""
                    SELECT table_name,
                           ROUND((data_length + index_length) / 1024) AS size_kb
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                    ORDER BY table_name
                """)
                rows = self._cursor.fetchall()
                return {"ok": True, "tables": [{"name": r[0], "size": f"{r[1] or 0} KB"} for r in rows]}

            elif self.db_type == "mongodb":
                db = self._mongo_db()
                collections = db.list_collection_names()
                result = []
                for col in collections:
                    # estimated_document_count es O(1) → no escanea la colección
                    count = db[col].estimated_document_count()
                    result.append({"name": col, "size": f"{count} docs"})
                return {"ok": True, "tables": result}

            elif self.db_type == "sqlite":
                self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                rows = self._cursor.fetchall()
                return {"ok": True, "tables": [{"name": r[0], "size": ""} for r in rows]}

            elif self.db_type == "redis":
                # Redis no tiene tablas; usamos el número de claves como "tamaño"
                keys_count = self._conn.dbsize()
                db_num = int(self.creds.get("database", 0))
                return {"ok": True, "tables": [{"name": f"DB {db_num}", "size": f"{keys_count} keys"}]}

            return {"ok": False, "error": f"Tipo no soportado: {self.db_type}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def execute_query(self, query: str, include_id: bool = False) -> dict:
        """Ejecuta una query y devuelve {columns, rows, rowcount}."""
        try:
            if self.db_type in ("postgresql", "mysql", "sqlite"):
                self._cursor.execute(query)
                q_upper = query.strip().upper()
                if q_upper.startswith(("SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "PRAGMA")):
                    # Query de lectura → devolver filas y columnas
                    rows = self._cursor.fetchall()
                    cols = [d[0] for d in (self._cursor.description or [])]
                    return {"ok": True, "columns": cols, "rows": [list(r) for r in rows],
                            "rowcount": len(rows)}
                else:
                    # Query de escritura → devolver número de filas afectadas
                    return {"ok": True, "columns": [], "rows": [],
                            "rowcount": self._cursor.rowcount,
                            "message": f"{self._cursor.rowcount} filas afectadas"}

            elif self.db_type == "mongodb":
                # Para MongoDB el "query" es un JSON como {"find": "users", "filter": {}}
                try:
                    cmd = json.loads(query)
                except json.JSONDecodeError:
                    return {"ok": False, "error": "Para MongoDB usa JSON. Ej: {\"find\": \"users\", \"filter\": {}}"}
                db = self._mongo_db()
                if include_id and "find" in cmd:
                    # find directo para controlar proyección de _id
                    col_name = cmd["find"]
                    filt     = cmd.get("filter", {})
                    limit    = cmd.get("limit", 1000)
                    docs     = list(db[col_name].find(filt).limit(limit))
                    for d in docs:
                        if "_id" in d:
                            d["_id"] = str(d["_id"])  # ObjectId no es serializable a JSON
                else:
                    result   = db.command(cmd)
                    docs     = result.get("cursor", {}).get("firstBatch", [result])
                    for d in docs:
                        if "_id" in d:
                            d["_id"] = str(d["_id"])
                        elif not include_id:
                            d.pop("_id", None)
                rows = [list(r.values()) for r in docs]
                cols = list(docs[0].keys()) if docs else []
                return {"ok": True, "columns": cols, "rows": rows, "rowcount": len(rows)}

            elif self.db_type == "redis":
                # Para Redis el "query" es un comando de texto como "GET mykey" o "KEYS *"
                parts = query.strip().split()
                result = self._conn.execute_command(*parts)
                if result is None:
                    return {"ok": True, "columns": ["result"], "rows": [["(nil)"]], "rowcount": 1}
                if isinstance(result, list):
                    return {"ok": True, "columns": ["value"], "rows": [[v] for v in result],
                            "rowcount": len(result)}
                return {"ok": True, "columns": ["result"], "rows": [[str(result)]], "rowcount": 1}

            return {"ok": False, "error": f"Tipo no soportado: {self.db_type}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def export_table(self, table_name: str, fmt: str = "json") -> dict:
        """Exporta hasta 10000 filas de una tabla a JSON o CSV."""
        try:
            if self.db_type in ("postgresql", "mysql", "sqlite"):
                self._cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 10000')
                rows = self._cursor.fetchall()
                cols = [d[0] for d in self._cursor.description]
                if fmt == "json":
                    data = [dict(zip(cols, r)) for r in rows]
                    return {"ok": True, "data": json.dumps(data, indent=2, default=str),
                            "filename": f"{table_name}.json"}
                else:  # csv
                    import io, csv
                    buf = io.StringIO()
                    w = csv.writer(buf)
                    w.writerow(cols)
                    w.writerows(rows)
                    return {"ok": True, "data": buf.getvalue(), "filename": f"{table_name}.csv"}

            elif self.db_type == "mongodb":
                db = self._mongo_db()
                # {"_id": 0} excluye el campo _id del resultado
                docs = list(db[table_name].find({}, {"_id": 0}).limit(10000))
                if fmt == "json":
                    return {"ok": True, "data": json.dumps(docs, indent=2, default=str),
                            "filename": f"{table_name}.json"}
                else:
                    if not docs:
                        return {"ok": True, "data": "", "filename": f"{table_name}.csv"}
                    import io, csv
                    buf = io.StringIO()
                    w = csv.DictWriter(buf, fieldnames=docs[0].keys())
                    w.writeheader()
                    w.writerows(docs)
                    return {"ok": True, "data": buf.getvalue(), "filename": f"{table_name}.csv"}

            elif self.db_type == "redis":
                # Exportar las primeras 1000 claves con su tipo y valor
                keys = self._conn.keys("*")[:1000]
                data = {}
                for k in keys:
                    ktype = self._conn.type(k)
                    if ktype == "string":
                        data[k] = self._conn.get(k)
                    elif ktype == "list":
                        data[k] = self._conn.lrange(k, 0, -1)
                    elif ktype == "set":
                        data[k] = list(self._conn.smembers(k))
                    elif ktype == "zset":
                        data[k] = self._conn.zrange(k, 0, -1, withscores=True)
                    elif ktype == "hash":
                        data[k] = self._conn.hgetall(k)
                    else:
                        data[k] = f"<{ktype}>"
                return {"ok": True, "data": json.dumps(data, indent=2, default=str), "filename": "redis_export.json"}

        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_table_schema(self, table_name: str) -> dict:
        """Obtiene la definición de columnas de una tabla (nombre, tipo, PK, nullable, default)."""
        try:
            if self.db_type == "postgresql":
                # JOIN a information_schema para detectar PK, UNIQUE, FK por columna
                self._cursor.execute("""
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        CASE
                            WHEN pk.column_name IS NOT NULL THEN 'PK'
                            WHEN uq.column_name IS NOT NULL THEN 'UNIQUE'
                            WHEN fk.column_name IS NOT NULL THEN 'FK'
                            ELSE NULL
                        END AS key_type
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT kcu.column_name FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_name = kcu.table_name
                        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
                    ) pk ON pk.column_name = c.column_name
                    LEFT JOIN (
                        SELECT kcu.column_name FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_name = kcu.table_name
                        WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = %s
                    ) uq ON uq.column_name = c.column_name
                    LEFT JOIN (
                        SELECT kcu.column_name FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_name = kcu.table_name
                        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s
                    ) fk ON fk.column_name = c.column_name
                    WHERE c.table_name = %s AND c.table_schema = 'public'
                    ORDER BY c.ordinal_position
                """, (table_name, table_name, table_name, table_name))
                rows = self._cursor.fetchall()
                cols = [{"name": r[0], "type": r[1], "nullable": r[2] == "YES",
                         "default": r[3], "key": r[4]} for r in rows]
                return {"ok": True, "columns": cols}

            elif self.db_type == "mysql":
                self._cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
                    FROM information_schema.COLUMNS
                    WHERE TABLE_NAME = %s AND TABLE_SCHEMA = DATABASE()
                    ORDER BY ORDINAL_POSITION
                """, (table_name,))
                rows = self._cursor.fetchall()
                # MySQL usa PRI/UNI/MUL en vez de PRIMARY KEY/UNIQUE/FOREIGN KEY
                key_map = {"PRI": "PK", "UNI": "UNIQUE", "MUL": "FK"}
                cols = [{"name": r[0], "type": r[1], "nullable": r[2] == "YES",
                         "default": r[3], "key": key_map.get(r[4])} for r in rows]
                return {"ok": True, "columns": cols}

            elif self.db_type == "sqlite":
                # PRAGMA table_info devuelve (cid, name, type, notnull, dflt_value, pk)
                self._cursor.execute(f'PRAGMA table_info("{table_name}")')
                rows = self._cursor.fetchall()
                cols = [{"name": r[1], "type": r[2] or "TEXT", "nullable": not r[3],
                         "default": r[4], "key": "PK" if r[5] else None} for r in rows]
                return {"ok": True, "columns": cols}

            elif self.db_type == "mongodb":
                db = self._mongo_db()
                # MongoDB no tiene esquema fijo → inferir tipos desde una muestra de 20 documentos
                sample = list(db[table_name].find({}, {"_id": 0}).limit(20))
                if not sample:
                    return {"ok": True, "columns": []}
                all_keys = {}
                for doc in sample:
                    for k, v in doc.items():
                        if k not in all_keys:
                            all_keys[k] = type(v).__name__  # usar el tipo Python como "tipo de columna"
                cols = [{"name": k, "type": t, "nullable": True, "default": None, "key": None}
                        for k, t in all_keys.items()]
                return {"ok": True, "columns": cols}

            return {"ok": True, "columns": []}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def insert_row(self, table: str, values: dict) -> dict:
        """Inserta una fila en la tabla usando los valores del dict."""
        try:
            if self.db_type in ("postgresql", "mysql", "sqlite"):
                cols         = list(values.keys())
                vals         = list(values.values())
                # PostgreSQL/MySQL usan %s; SQLite usa ?
                ph           = "%s" if self.db_type != "sqlite" else "?"
                placeholders = ", ".join([ph] * len(cols))
                col_names    = ", ".join(f'"{c}"' for c in cols)
                self._cursor.execute(
                    f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})', vals
                )
                if self.db_type == "sqlite":
                    self._conn.commit()  # SQLite no tiene autocommit por defecto
                return {"ok": True, "rowcount": self._cursor.rowcount}

            elif self.db_type == "mongodb":
                db     = self._mongo_db()
                result = db[table].insert_one(values)
                return {"ok": True, "inserted_id": str(result.inserted_id)}

            elif self.db_type == "redis":
                # En Redis, "insertar" es SET key value
                key = values.get("key", "").strip()
                val = values.get("value", "")
                if not key:
                    return {"ok": False, "error": "El campo 'key' es obligatorio"}
                self._conn.set(key, val)
                return {"ok": True}

            return {"ok": False, "error": f"Tipo no soportado: {self.db_type}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def update_row(self, table: str, pk_col: str, pk_val, values: dict) -> dict:
        """Actualiza una fila identificada por pk_col=pk_val."""
        try:
            if self.db_type in ("postgresql", "mysql", "sqlite"):
                ph   = "%s" if self.db_type != "sqlite" else "?"
                sets = ", ".join(f'"{c}" = {ph}' for c in values)
                vals = list(values.values()) + [pk_val]
                self._cursor.execute(
                    f'UPDATE "{table}" SET {sets} WHERE "{pk_col}" = {ph}', vals
                )
                if self.db_type == "sqlite":
                    self._conn.commit()
                return {"ok": True, "rowcount": self._cursor.rowcount}

            elif self.db_type == "mongodb":
                from bson import ObjectId
                db = self._mongo_db()
                # Intentar convertir pk_val a ObjectId (formato nativo de _id en MongoDB)
                try:
                    filter_val = ObjectId(pk_val)
                except Exception:
                    filter_val = pk_val
                result = db[table].update_one({"_id": filter_val}, {"$set": values})
                return {"ok": True, "rowcount": result.modified_count}

            elif self.db_type == "redis":
                # pk_val es la clave Redis; values tiene el nuevo valor
                new_val = values.get("value", "")
                ktype   = self._conn.type(pk_val)
                if ktype == "string" or ktype == "none":
                    self._conn.set(pk_val, new_val)
                elif ktype == "hash":
                    self._conn.hset(pk_val, mapping={k: v for k, v in values.items()})
                else:
                    return {"ok": False, "error": f"Edición directa no soportada para tipo Redis '{ktype}'"}
                return {"ok": True}

            return {"ok": False, "error": f"Tipo no soportado: {self.db_type}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def delete_row(self, table: str, pk_col: str, pk_val) -> dict:
        """Elimina la fila donde pk_col=pk_val."""
        try:
            if self.db_type in ("postgresql", "mysql", "sqlite"):
                ph = "%s" if self.db_type != "sqlite" else "?"
                self._cursor.execute(f'DELETE FROM "{table}" WHERE "{pk_col}" = {ph}', [pk_val])
                if self.db_type == "sqlite":
                    self._conn.commit()
                return {"ok": True, "rowcount": self._cursor.rowcount}

            elif self.db_type == "mongodb":
                from bson import ObjectId
                db = self._mongo_db()
                try:
                    filter_val = ObjectId(pk_val)
                except Exception:
                    filter_val = pk_val
                result = db[table].delete_one({"_id": filter_val})
                return {"ok": True, "rowcount": result.deleted_count}

            elif self.db_type == "redis":
                deleted = self._conn.delete(pk_val)
                return {"ok": True, "rowcount": deleted}

            return {"ok": False, "error": f"Tipo no soportado: {self.db_type}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_server_info(self) -> dict:
        """Obtiene versión y metadata del servidor de BD."""
        try:
            if self.db_type == "postgresql":
                self._cursor.execute("SELECT version()")
                ver = self._cursor.fetchone()[0]
                self._cursor.execute("SELECT pg_database_size(current_database())")
                size = self._cursor.fetchone()[0]
                return {"ok": True, "version": ver, "size": f"{size // (1024*1024)} MB"}

            elif self.db_type == "mysql":
                self._cursor.execute("SELECT VERSION()")
                ver = self._cursor.fetchone()[0]
                return {"ok": True, "version": ver}

            elif self.db_type == "mongodb":
                info = self._conn.server_info()
                return {"ok": True, "version": info.get("version", ""), "extra": info}

            elif self.db_type == "sqlite":
                import sqlite3
                return {"ok": True, "version": sqlite3.sqlite_version}

            elif self.db_type == "redis":
                # info("server") devuelve metadatos del servidor Redis
                info = self._conn.info("server")
                return {"ok": True, "version": info.get("redis_version", ""),
                        "uptime": info.get("uptime_in_seconds", 0)}

            return {"ok": False, "error": f"Tipo no soportado: {self.db_type}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

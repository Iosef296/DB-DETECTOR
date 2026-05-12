# 🔍 DB Detector

Detecta automáticamente el tipo de base de datos de cualquier proyecto
y se conecta con las credenciales encontradas.

## Requisitos
- Python 3.8+

## Arrancar

```bash
python run.py
```

Abre automáticamente http://localhost:7432

## ¿Qué detecta?

| Base de datos | Señales buscadas |
|---|---|
| PostgreSQL | `psycopg2`, `pg`, `postgres://`, `.env`, `docker-compose` |
| MySQL / MariaDB | `mysql2`, `pymysql`, `mysql://` |
| MongoDB | `mongoose`, `pymongo`, `mongodb://` |
| SQLite | `sqlite3`, `better-sqlite3`, `sqlite:///` |
| Redis | `redis`, `ioredis`, `redis://` |

## Archivos analizados

- Variables de entorno: `.env`, `.env.*`, `docker-compose.yml`
- Dependencias: `package.json`, `requirements.txt`, `Pipfile`, `pyproject.toml`, `go.mod`, `Gemfile`, `pom.xml`, `composer.json`
- Código fuente: `.py`, `.js`, `.ts`, `.rb`, `.go`, `.java`, `.php`
- Configs: `appsettings.json`, `application.properties`, `database.yml`

## Funcionalidades

- ✅ Detección automática del tipo de BD
- ✅ Extracción de credenciales del proyecto
- ✅ Formulario editable para ajustar credenciales
- ✅ Ver tablas / colecciones con tamaños
- ✅ Ejecutar queries (SQL, MongoDB JSON, Redis commands)
- ✅ Exportar tablas en JSON o CSV
- ✅ Evidencia detallada de por qué se detectó cada BD

## Puerto

Por defecto usa el puerto **7432**. Puedes cambiarlo:

```bash
PORT=8080 python run.py
```

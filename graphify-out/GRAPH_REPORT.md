# Graph Report - /home/iosef/SISTEMAS/db-detector  (2026-05-08)

## Corpus Check
- Corpus is ~22,597 words - fits in a single context window. You may not need a graph.

## Summary
- 169 nodes · 221 edges · 16 communities (9 shown, 7 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 7 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Flask REST API Layer|Flask REST API Layer]]
- [[_COMMUNITY_Database Detection Engine|Database Detection Engine]]
- [[_COMMUNITY_Cross-Layer Bridge|Cross-Layer Bridge]]
- [[_COMMUNITY_DB Connector Core|DB Connector Core]]
- [[_COMMUNITY_Credential Pattern Matching|Credential Pattern Matching]]
- [[_COMMUNITY_Table Operations & CRUD|Table Operations & CRUD]]
- [[_COMMUNITY_Driver Loading & Connection|Driver Loading & Connection]]
- [[_COMMUNITY_Startup & Entry Point|Startup & Entry Point]]
- [[_COMMUNITY_App Launcher|App Launcher]]
- [[_COMMUNITY_Flask Application|Flask Application]]
- [[_COMMUNITY_Driver Install API|Driver Install API]]
- [[_COMMUNITY_Server Install API|Server Install API]]
- [[_COMMUNITY_Native Server Install|Native Server Install]]
- [[_COMMUNITY_Cloud Discovery|Cloud Discovery]]
- [[_COMMUNITY_Fly.io Proxy|Fly.io Proxy]]

## God Nodes (most connected - your core abstractions)
1. `DBConnection` - 20 edges
2. `DatabaseDetector.scan_env_files` - 12 edges
3. `DatabaseDetector` - 11 edges
4. `_state (in-memory session)` - 9 edges
5. `DBConnection.connect` - 7 edges
6. `DBConnection._mongo_db` - 7 edges
7. `DatabaseDetector.detect` - 6 edges
8. `DatabaseDetector.scan_dependency_files` - 6 edges
9. `DatabaseDetector.scan_source_files` - 6 edges
10. `api_connect` - 6 edges

## Surprising Connections (you probably didn't know these)
- `DB Detector (project)` --references--> `DBConnection`  [INFERRED]
  README.md → connector.py
- `DatabaseDetector` --semantically_similar_to--> `DBConnection`  [INFERRED] [semantically similar]
  detector.py → connector.py
- `DBDetector Frontend (index.html)` --references--> `api_detect`  [INFERRED]
  static/index.html → server.py
- `DBDetector Frontend (index.html)` --references--> `api_connect`  [INFERRED]
  static/index.html → server.py
- `DBDetector Frontend (index.html)` --references--> `api_query`  [INFERRED]
  static/index.html → server.py

## Hyperedges (group relationships)
- **DB Detection to Connection Pipeline** — detector_databasedetector, connector_dbconnection, server_state [INFERRED 0.95]
- **Multi-source Credential Extraction Pipeline** — detector_scan_env_files, detector_scan_dependency_files, detector_scan_source_files [EXTRACTED 1.00]
- **Docker Compose Integration Flow** — server_api_compose_up, server_patch_compose_ports, server_extract_compose_db_info [EXTRACTED 1.00]

## Communities (16 total, 7 thin omitted)

### Community 0 - "Flask REST API Layer"
Cohesion: 0.06
Nodes (22): api_app_install_runtime(), api_app_start(), api_cloud_discover(), api_compose_up(), api_connect(), api_fly_proxy(), _extract_compose_db_info(), _find_free_port() (+14 more)

### Community 1 - "Database Detection Engine"
Cohesion: 0.17
Nodes (13): DatabaseDetector, extract_individual_env_vars(), extract_spring_datasource(), find_files(), parse_url_credentials(), Extrae credenciales individuales como DB_HOST, DB_USER, etc., Extrae credenciales del formato Spring Boot (application.yaml / .properties)., Parsea archivos de configuración de plataformas cloud. (+5 more)

### Community 2 - "Cross-Layer Bridge"
Cohesion: 0.1
Nodes (23): DBConnection, DBConnection.execute_query, DBConnection.get_server_info, DatabaseDetector, DBDetector Frontend (index.html), DB Detector (project), api_app_install_runtime, api_app_logs (+15 more)

### Community 3 - "DB Connector Core"
Cohesion: 0.15
Nodes (3): DBConnection, _is_server_down(), Devuelve el objeto db de Mongo, con fallback a la primera BD disponible.

### Community 4 - "Credential Pattern Matching"
Cohesion: 0.19
Nodes (18): Multi-source Credential Detection Strategy, DatabaseDetector._add_evidence, COMPOSE_IMAGE_PATTERNS, DB_URL_PATTERNS, DatabaseDetector.detect, DatabaseDetector._detect_app_start, DRIVER_PATTERNS, ENV_KEY_PATTERNS (+10 more)

### Community 5 - "Table Operations & CRUD"
Cohesion: 0.15
Nodes (13): DBConnection.delete_row, DBConnection.export_table, DBConnection.get_table_schema, DBConnection.insert_row, DBConnection.list_tables, DBConnection._mongo_db, DBConnection.update_row, api_delete (+5 more)

### Community 6 - "Driver Loading & Connection"
Cohesion: 0.24
Nodes (11): DBConnection._attach_install_hint, DBConnection.connect, DBConnection._connect_mongo, DBConnection._connect_mysql, DBConnection._connect_postgres, DBConnection._connect_redis, DBConnection._connect_sqlite, _DRIVER_HINTS (+3 more)

### Community 9 - "App Launcher"
Cohesion: 0.67
Nodes (3): install_deps, run.main, server.main

## Knowledge Gaps
- **51 isolated node(s):** `Devuelve el objeto db de Mongo, con fallback a la primera BD disponible.`, `Extrae credenciales individuales como DB_HOST, DB_USER, etc.`, `Extrae credenciales del formato Spring Boot (application.yaml / .properties).`, `Parsea archivos de configuración de plataformas cloud.`, `Escanea archivos de código fuente buscando strings de conexión.` (+46 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DBConnection` connect `DB Connector Core` to `Flask REST API Layer`?**
  _High betweenness centrality (0.109) - this node is a cross-community bridge._
- **Why does `api_detect()` connect `Database Detection Engine` to `Flask REST API Layer`?**
  _High betweenness centrality (0.100) - this node is a cross-community bridge._
- **What connects `Devuelve el objeto db de Mongo, con fallback a la primera BD disponible.`, `Extrae credenciales individuales como DB_HOST, DB_USER, etc.`, `Extrae credenciales del formato Spring Boot (application.yaml / .properties).` to the rest of the system?**
  _51 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Flask REST API Layer` be split into smaller, more focused modules?**
  _Cohesion score 0.06 - nodes in this community are weakly interconnected._
- **Should `Cross-Layer Bridge` be split into smaller, more focused modules?**
  _Cohesion score 0.1 - nodes in this community are weakly interconnected._
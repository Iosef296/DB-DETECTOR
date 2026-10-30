"""Liberar RAM igualando el perfil real de Mem Reduct del usuario (el que le
baja la RAM de 80% a 40%), verificado contra el código fuente de esa
herramienta (github.com/henrypp/memreduct): Working set, System file cache,
Standby list sin prioridad, Modified file cache y Registry cache/Combine
memory lists. Deliberadamente NO incluye "Modified page list" ni "Standby
list" (purga completa) — esas dos están desactivadas en el perfil real del
usuario porque son las más agresivas (fuerzan I/O de disco inmediato / tiran
toda la cache de lectura, no solo la de baja prioridad).
"""
import ctypes
import ctypes.wintypes as wintypes
import platform
import sys

PROCESS_QUERY_INFORMATION = 0x0400
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
PROCESS_SET_QUOTA = 0x0100

SE_PRIVILEGE_ENABLED = 0x00000002
TOKEN_ADJUST_PRIVILEGES = 0x0020
TOKEN_QUERY = 0x0008

# SYSTEM_INFORMATION_CLASS (ver github.com/henrypp/routine src/ntapi.h)
SystemFileCacheInformationEx = 81
SystemMemoryListInformation = 80
SystemCombinePhysicalMemoryInformation = 130
SystemRegistryReconciliationInformation = 155

# SYSTEM_MEMORY_LIST_COMMAND (mismo header, enum 0-indexado)
MemoryFlushModifiedList = 3
MemoryPurgeStandbyList = 4
MemoryPurgeLowPriorityStandbyList = 5

MAXSIZE_T = ctypes.c_size_t(-1).value

# SYSTEM_MEMORY_LIST_COMMAND (mismo header que arriba, 0-indexado)
MemoryEmptyWorkingSets = 2


class _MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


def _free_mb() -> float:
    stat = _MEMORYSTATUSEX()
    stat.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
    return round(stat.ullAvailPhys / (1024 * 1024), 1)


def mem_load_percent() -> int:
    """% de RAM física en uso ahora mismo (dwMemoryLoad de GlobalMemoryStatusEx)."""
    stat = _MEMORYSTATUSEX()
    stat.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
    return stat.dwMemoryLoad


def _windows_version() -> tuple:
    v = sys.getwindowsversion()
    return (v.major, v.minor)


class _LUID(ctypes.Structure):
    _fields_ = [("LowPart", wintypes.DWORD), ("HighPart", wintypes.LONG)]


class _LUID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("Luid", _LUID), ("Attributes", wintypes.DWORD)]


class _TOKEN_PRIVILEGES(ctypes.Structure):
    _fields_ = [("PrivilegeCount", wintypes.DWORD), ("Privileges", _LUID_AND_ATTRIBUTES * 1)]


class _SYSTEM_FILECACHE_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("CurrentSize", ctypes.c_size_t),
        ("PeakSize", ctypes.c_size_t),
        ("PageFaultCount", ctypes.c_ulong),
        ("MinimumWorkingSet", ctypes.c_size_t),
        ("MaximumWorkingSet", ctypes.c_size_t),
        ("CurrentSizeIncludingTransitionInPages", ctypes.c_size_t),
        ("PeakSizeIncludingTransitionInPages", ctypes.c_size_t),
        ("TransitionRePurposeCount", ctypes.c_ulong),
        ("Flags", ctypes.c_ulong),
    ]


class _MEMORY_COMBINE_INFORMATION_EX(ctypes.Structure):
    _fields_ = [
        ("Handle", wintypes.HANDLE),
        ("PagesCombined", ctypes.c_size_t),
        ("Flags", ctypes.c_ulong),
    ]


def _acquire_privilege(name: str):
    """Habilita un privilegio en el token del proceso actual. Devuelve
    (handle_del_token, None) si funcionó, o (None, razón) si no — el caller
    es responsable de cerrar el handle con CloseHandle."""
    advapi32 = ctypes.windll.advapi32
    kernel32 = ctypes.windll.kernel32

    # ctypes trunca punteros/handles a 32 bits si no se declara restype
    # explícito en una función que por default asume c_int — en 64 bits esto
    # corrompe el handle silenciosamente y OpenProcessToken falla siempre.
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    advapi32.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]

    h_token = wintypes.HANDLE()
    if not advapi32.OpenProcessToken(
        kernel32.GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, ctypes.byref(h_token)
    ):
        return None, "No se pudo abrir el token del proceso"

    luid = _LUID()
    if not advapi32.LookupPrivilegeValueW(None, name, ctypes.byref(luid)):
        kernel32.CloseHandle(h_token)
        return None, "LookupPrivilegeValue falló"

    tp = _TOKEN_PRIVILEGES()
    tp.PrivilegeCount = 1
    tp.Privileges[0].Luid = luid
    tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED
    advapi32.AdjustTokenPrivileges.argtypes = [
        wintypes.HANDLE, wintypes.BOOL, ctypes.POINTER(_TOKEN_PRIVILEGES),
        wintypes.DWORD, ctypes.c_void_p, ctypes.c_void_p,
    ]
    advapi32.AdjustTokenPrivileges(h_token, False, ctypes.byref(tp), 0, None, None)
    if kernel32.GetLastError() != 0:
        kernel32.CloseHandle(h_token)
        return None, "Necesitás ejecutar el server como Administrador"
    return h_token, None


def _nt_set_system_information(info_class: int, privilege_name: str, buf=None, size: int = 0) -> dict:
    kernel32 = ctypes.windll.kernel32
    ntdll = ctypes.windll.ntdll

    h_token, err = _acquire_privilege(privilege_name)
    if err:
        return {"applied": False, "reason": err}

    try:
        status = ntdll.NtSetSystemInformation(info_class, buf, size)
        if status != 0:
            return {"applied": False, "reason": f"NtSetSystemInformation devolvió 0x{status & 0xFFFFFFFF:08X} (¿no sos Administrador?)"}
        return {"applied": True}
    finally:
        kernel32.CloseHandle(h_token)


def _empty_working_sets_global() -> dict:
    """"Working set" de Mem Reduct: UN solo NtSetSystemInformation que vacía
    el working set de TODOS los procesos del sistema, incluidos los
    protegidos/SYSTEM que un OpenProcess por handle no puede tocar. Esto
    incluye java.exe (microservicios) — a diferencia de la versión anterior,
    ya no se excluye, para igualar el alcance real de Mem Reduct."""
    cmd = wintypes.DWORD(MemoryEmptyWorkingSets)
    return _nt_set_system_information(
        SystemMemoryListInformation, "SeProfileSingleProcessPrivilege", ctypes.byref(cmd), ctypes.sizeof(cmd)
    )


def _purge_standby_list_low_priority() -> dict:
    """"Standby list sin prioridad" de Mem Reduct — purga solo las páginas de
    baja prioridad del standby list (cache de lectura reciente), a diferencia
    del purge completo (más agresivo, desactivado en el perfil real del
    usuario)."""
    cmd = wintypes.DWORD(MemoryPurgeLowPriorityStandbyList)
    return _nt_set_system_information(
        SystemMemoryListInformation, "SeProfileSingleProcessPrivilege", ctypes.byref(cmd), ctypes.sizeof(cmd)
    )


def _flush_system_file_cache() -> dict:
    """"System file cache" de Mem Reduct: fuerza al working set del cache de
    archivos del sistema a su mínimo posible."""
    info = _SYSTEM_FILECACHE_INFORMATION()
    info.MinimumWorkingSet = MAXSIZE_T
    info.MaximumWorkingSet = MAXSIZE_T
    return _nt_set_system_information(
        SystemFileCacheInformationEx, "SeIncreaseQuotaPrivilege", ctypes.byref(info), ctypes.sizeof(info)
    )


def _flush_registry_cache() -> dict:
    """"Registry cache (win8.1+)" de Mem Reduct: fuerza a reconciliar/flushear
    los hives de registro. No disponible antes de Windows 8.1."""
    if _windows_version() < (6, 3):
        return {"applied": False, "reason": "Requiere Windows 8.1 o superior"}
    return _nt_set_system_information(SystemRegistryReconciliationInformation, "SeProfileSingleProcessPrivilege")


def _combine_memory_lists() -> dict:
    """"Combine memory lists (win10+)" de Mem Reduct: deduplica páginas
    físicas idénticas entre procesos. No disponible antes de Windows 10."""
    if _windows_version() < (10, 0):
        return {"applied": False, "reason": "Requiere Windows 10 o superior"}
    info = _MEMORY_COMBINE_INFORMATION_EX()
    return _nt_set_system_information(
        SystemCombinePhysicalMemoryInformation, "SeProfileSingleProcessPrivilege", ctypes.byref(info), ctypes.sizeof(info)
    )


def _flush_volume_caches() -> dict:
    """"Modified file cache" de Mem Reduct: Mem Reduct lo hace a bajo nivel
    (NtCreateFile + mount manager), acá usamos el equivalente documentado de
    Win32 — enumerar volúmenes y flushear cada uno con FlushFileBuffers,
    mismo efecto (vacía páginas modificadas de archivos a disco)."""
    kernel32 = ctypes.windll.kernel32
    GENERIC_WRITE = 0x40000000
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    OPEN_EXISTING = 3
    INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.FindFirstVolumeW.restype = wintypes.HANDLE
    kernel32.FindNextVolumeW.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD]
    kernel32.FindVolumeClose.argtypes = [wintypes.HANDLE]

    buf = ctypes.create_unicode_buffer(260)
    h_find = kernel32.FindFirstVolumeW(buf, 260)
    if h_find == INVALID_HANDLE_VALUE:
        return {"applied": False, "reason": "FindFirstVolumeW falló"}

    flushed = 0
    try:
        while True:
            volume_path = buf.value.rstrip("\\")
            handle = kernel32.CreateFileW(
                volume_path, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, None, OPEN_EXISTING, 0, None
            )
            if handle and handle != INVALID_HANDLE_VALUE:
                try:
                    if kernel32.FlushFileBuffers(handle):
                        flushed += 1
                finally:
                    kernel32.CloseHandle(handle)
            if not kernel32.FindNextVolumeW(h_find, buf, 260):
                break
    finally:
        kernel32.FindVolumeClose(h_find)

    if flushed == 0:
        return {"applied": False, "reason": "No se pudo flushear ningún volumen (¿no sos Administrador?)"}
    return {"applied": True, "volumes_flushed": flushed}


def _process_name(kernel32, handle) -> str:
    buf = ctypes.create_unicode_buffer(260)
    size = wintypes.DWORD(260)
    if kernel32.QueryFullProcessImageNameW(handle, 0, buf, ctypes.byref(size)):
        return buf.value.rsplit("\\", 1)[-1]
    return ""


# Ejecutables de BD/runtime que esta app puede dejar huérfanos (postgres con
# workers auxiliares sin puerto propio, java de mvnw que sobrevive a un
# restart del server, etc.) — los únicos que tiene sentido mostrar para que
# el usuario los mate a mano; el resto de los procesos del sistema no son
# asunto nuestro.
_DB_PROCESS_NAMES = ("java.exe", "postgres.exe", "mysqld.exe", "mongod.exe", "redis-server.exe")


class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def list_db_processes() -> dict:
    """Lista los procesos java/postgres/mysqld/mongod/redis-server vivos ahora
    mismo, con su RAM, para que el usuario los mate a mano si quedaron
    huérfanos (workers de Postgres sin puerto propio, java de un restart
    anterior, etc. — el mismo tipo de restos que venimos limpiando a mano)."""
    if platform.system() != "Windows":
        return {"ok": False, "error": "Solo soportado en Windows"}

    psapi = ctypes.windll.psapi
    kernel32 = ctypes.windll.kernel32

    arr_size = 8192
    pids = (wintypes.DWORD * arr_size)()
    bytes_returned = wintypes.DWORD()
    if not psapi.EnumProcesses(ctypes.byref(pids), ctypes.sizeof(pids), ctypes.byref(bytes_returned)):
        return {"ok": False, "error": "EnumProcesses falló"}
    count = bytes_returned.value // ctypes.sizeof(wintypes.DWORD)

    flags = PROCESS_QUERY_INFORMATION | PROCESS_QUERY_LIMITED_INFORMATION
    procs = []
    for i in range(count):
        pid = pids[i]
        if pid == 0:
            continue
        handle = kernel32.OpenProcess(flags, False, pid)
        if not handle:
            continue
        try:
            name = _process_name(kernel32, handle)
            if name.lower() not in _DB_PROCESS_NAMES:
                continue
            pmc = _PROCESS_MEMORY_COUNTERS()
            pmc.cb = ctypes.sizeof(pmc)
            mb = 0.0
            if psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), ctypes.sizeof(pmc)):
                mb = round(pmc.WorkingSetSize / (1024 * 1024), 1)
            procs.append({"pid": pid, "name": name, "mb": mb})
        except Exception:
            pass
        finally:
            kernel32.CloseHandle(handle)

    procs.sort(key=lambda p: (p["name"].lower(), -p["mb"]))
    return {"ok": True, "processes": procs}


def is_admin() -> bool:
    """True si el proceso actual corre elevado. Sin esto, system file cache,
    standby list, modified cache, registry cache y combine memory lists
    fallan silenciosos (solo AdjustTokenPrivileges, sin excepción) y queda
    activo nomás el working set trim."""
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def free_ram() -> dict:
    if platform.system() != "Windows":
        return {"ok": False, "error": "Solo soportado en Windows"}

    kernel32 = ctypes.windll.kernel32
    admin = is_admin()

    mem_before = _free_mb()

    arr_size = 8192
    pids = (wintypes.DWORD * arr_size)()
    bytes_returned = wintypes.DWORD()
    psapi = ctypes.windll.psapi
    if not psapi.EnumProcesses(ctypes.byref(pids), ctypes.sizeof(pids), ctypes.byref(bytes_returned)):
        return {"ok": False, "error": "EnumProcesses falló"}
    count = bytes_returned.value // ctypes.sizeof(wintypes.DWORD)

    # Perfil real de Mem Reduct del usuario (el que le baja la RAM de 80% a
    # 40%): Working set, System file cache, Standby list sin prioridad,
    # Modified file cache, Registry cache y Combine memory lists. A propósito
    # NO incluye Modified page list ni Standby list (purga completa) —
    # desactivados en ese perfil por ser los más agresivos.
    working_set = _empty_working_sets_global()
    system_cache = _flush_system_file_cache()
    standby_low = _purge_standby_list_low_priority()
    volume_cache = _flush_volume_caches()
    registry_cache = _flush_registry_cache()
    combine_lists = _combine_memory_lists()

    mem_after = _free_mb()
    return {
        "ok": True,
        "is_admin": admin,
        "total_procesos": count,
        "free_mb_before": mem_before,
        "free_mb_after": mem_after,
        "freed_mb": round(mem_after - mem_before, 1),
        "working_set_emptied": working_set["applied"],
        "working_set_note": working_set.get("reason"),
        "system_file_cache_flushed": system_cache["applied"],
        "system_file_cache_note": system_cache.get("reason"),
        "standby_list_low_priority_purged": standby_low["applied"],
        "standby_list_low_priority_note": standby_low.get("reason"),
        "volume_cache_flushed": volume_cache["applied"],
        "volume_cache_note": volume_cache.get("reason"),
        "registry_cache_flushed": registry_cache["applied"],
        "registry_cache_note": registry_cache.get("reason"),
        "memory_lists_combined": combine_lists["applied"],
        "memory_lists_combined_note": combine_lists.get("reason"),
    }

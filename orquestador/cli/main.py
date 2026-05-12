import sys
import time
from pathlib import Path

# Ensure parent in path
sys.path.insert(0, str(Path(__file__).parent.parent))

import click
from rich.console import Console
from rich.table import Table
from rich import box

from core.orchestrator import Orchestrator

_DB_PATH = str(Path(__file__).parent.parent / "storage" / "projects.db")

console = Console()

_STATUS_COLOR = {
    "RUNNING":  "green",
    "STOPPED":  "dim white",
    "STARTING": "yellow",
    "ERROR":    "red",
}

_DB_ICON = {
    "postgresql": "[blue]PG[/blue]",
    "mysql":      "[cyan]MY[/cyan]",
    "mongodb":    "[green]MG[/green]",
    "redis":      "[red]RD[/red]",
    "sqlite":     "[yellow]SQ[/yellow]",
}


def _get_orch() -> Orchestrator:
    return Orchestrator(_DB_PATH)


@click.group()
def cli():
    """Orquestador de entornos de desarrollo multi-proyecto."""
    pass


@cli.command("add")
@click.argument("ruta")
def cmd_add(ruta):
    """Registra un proyecto nuevo."""
    orch   = _get_orch()
    result = orch.add_project(ruta)
    if not result.get("ok"):
        console.print(f"[red]Error:[/red] {result.get('error')}")
        sys.exit(1)
    db_type   = result.get("db_type") or "desconocida"
    framework = result.get("framework") or "desconocido"
    app_port  = result.get("app_port")
    db_port   = result.get("db_port")
    console.print(f"[green]Proyecto '{result['name']}' registrado[/green]")
    console.print(f"  BD: [cyan]{db_type.upper()}[/cyan]  |  Framework: [cyan]{framework}[/cyan]")
    console.print(f"  App: localhost:{app_port}  |  DB: localhost:{db_port}")


@cli.command("list")
def cmd_list():
    """Lista todos los proyectos."""
    orch     = _get_orch()
    projects = orch.list_projects()
    if not projects:
        console.print("[dim]No hay proyectos registrados. Usa 'add <ruta>' para agregar uno.[/dim]")
        return
    table = Table(box=box.ROUNDED, show_header=True, header_style="bold")
    table.add_column("Nombre")
    table.add_column("BD")
    table.add_column("Framework")
    table.add_column("Puerto App")
    table.add_column("Puerto DB")
    table.add_column("Estado")
    for p in projects:
        status    = p.get("status", "STOPPED")
        color     = _STATUS_COLOR.get(status, "white")
        db_type   = p.get("db_type") or "-"
        framework = p.get("framework") or "-"
        table.add_row(
            p["name"],
            db_type,
            framework,
            str(p.get("app_port") or "-"),
            str(p.get("db_port") or "-"),
            f"[{color}]{status}[/{color}]",
        )
    console.print(table)


@cli.command("up")
@click.argument("nombre", required=False)
@click.option("--all", "all_", is_flag=True, help="Levanta todos los proyectos en STOPPED")
def cmd_up(nombre, all_):
    """Levanta un proyecto (o todos con --all)."""
    orch = _get_orch()
    if all_:
        result = orch.up_all()
        for n in result.get("started", []):
            console.print(f"[green]{n}[/green] levantado")
        for e in result.get("errors", []):
            console.print(f"[red]{e['name']}[/red]: {e['error']}")
        return
    if not nombre:
        console.print("[red]Proporciona un nombre o usa --all[/red]")
        sys.exit(1)
    console.print(f"[yellow]Levantando Docker para '{nombre}'...[/yellow]", end=" ")
    result = orch.up(nombre)
    if result.get("ok"):
        console.print("[green]OK[/green]")
        console.print(f"[green]'{nombre}' corriendo en {result.get('app_url', '')}[/green]")
    else:
        console.print("[red]FALLO[/red]")
        console.print(f"[red]Error:[/red] {result.get('error')}")
        sys.exit(1)


@cli.command("down")
@click.argument("nombre", required=False)
@click.option("--all", "all_", is_flag=True, help="Para todos los proyectos en RUNNING")
def cmd_down(nombre, all_):
    """Para un proyecto (o todos con --all)."""
    orch = _get_orch()
    if all_:
        result = orch.down_all()
        for n in result.get("stopped", []):
            console.print(f"[dim]{n}[/dim] detenido")
        for e in result.get("errors", []):
            console.print(f"[red]{e['name']}[/red]: {e['error']}")
        return
    if not nombre:
        console.print("[red]Proporciona un nombre o usa --all[/red]")
        sys.exit(1)
    result = orch.down(nombre)
    if result.get("ok"):
        console.print(f"[dim]'{nombre}' detenido[/dim]")
    else:
        console.print(f"[red]Error:[/red] {result.get('error')}")
        sys.exit(1)


@cli.command("restart")
@click.argument("nombre")
def cmd_restart(nombre):
    """Reinicia un proyecto."""
    orch   = _get_orch()
    result = orch.restart(nombre)
    if result.get("ok"):
        console.print(f"[green]'{nombre}' reiniciado en {result.get('app_url', '')}[/green]")
    else:
        console.print(f"[red]Error:[/red] {result.get('error')}")
        sys.exit(1)


@cli.command("status")
@click.argument("nombre")
def cmd_status(nombre):
    """Muestra detalle completo de un proyecto."""
    orch   = _get_orch()
    row    = orch.status(nombre)
    if not row:
        console.print(f"[red]Proyecto '{nombre}' no encontrado[/red]")
        sys.exit(1)
    status    = row.get("status", "STOPPED")
    color     = _STATUS_COLOR.get(status, "white")
    console.print(f"\n[bold]{row['name']}[/bold]")
    console.print(f"  Ruta:       {row.get('path', '-')}")
    console.print(f"  BD:         {row.get('db_type') or '-'}")
    console.print(f"  Framework:  {row.get('framework') or '-'}")
    console.print(f"  Puerto App: {row.get('app_port') or '-'}")
    console.print(f"  Puerto DB:  {row.get('db_port') or '-'}")
    console.print(f"  Estado:     [{color}]{status}[/{color}]")
    console.print(f"  Actualizado: {row.get('updated_at', '-')}")
    docker_info = row.get("docker") or {}
    if docker_info:
        console.print(f"\n  [bold]Contenedores Docker:[/bold]")
        for svc, state in docker_info.items():
            console.print(f"    {svc}: {state}")


@cli.command("logs")
@click.argument("nombre")
@click.option("--lines", "-n", default=50, help="Cantidad de líneas")
@click.option("--follow", "-f", is_flag=True, help="Seguir en tiempo real (polling 2s)")
def cmd_logs(nombre, lines, follow):
    """Muestra logs de un proyecto."""
    orch = _get_orch()
    if not follow:
        logs = orch.get_logs(nombre, lines)
        for line in logs:
            console.print(line)
        return
    seen = 0
    try:
        while True:
            logs = orch.get_logs(nombre, lines)
            for line in logs[seen:]:
                console.print(line)
            seen = len(logs)
            time.sleep(2)
    except KeyboardInterrupt:
        pass


@cli.command("remove")
@click.argument("nombre")
def cmd_remove(nombre):
    """Elimina un proyecto del orquestador."""
    confirm = click.prompt(
        f"Seguro? Esto no elimina los archivos de '{nombre}'. [s/N]",
        default="N"
    )
    if confirm.strip().lower() not in ("s", "si", "y", "yes"):
        console.print("[dim]Cancelado[/dim]")
        return
    orch   = _get_orch()
    result = orch.remove_project(nombre)
    if result.get("ok"):
        console.print(f"[dim]'{nombre}' eliminado del orquestador[/dim]")
    else:
        console.print(f"[red]Error:[/red] {result.get('error')}")
        sys.exit(1)


@cli.command("web")
def cmd_web():
    """Abre el dashboard web en http://localhost:7433."""
    import socket
    import webbrowser
    import subprocess as _sp

    url  = "http://localhost:7433"
    host = "localhost"
    port = 7433

    def _running():
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            return False

    if not _running():
        console.print("[yellow]Iniciando servidor...[/yellow]")
        run_py = str(Path(__file__).parent.parent / "run.py")
        _sp.Popen([sys.executable, run_py, "--no-browser"], close_fds=True)
        for _ in range(15):
            time.sleep(1)
            if _running():
                break
        else:
            console.print(f"[red]No se pudo iniciar el servidor en {url}[/red]")
            sys.exit(1)

    webbrowser.open(url)
    console.print(f"[green]Abierto: {url}[/green]")


if __name__ == "__main__":
    cli()

import click
from pathlib import Path
from importlib import resources

def scaffold_from_template(template_path, target_root: Path, domain_name: str):

    for item in template_path.iterdir():
        clean_name = item.name.replace(".mako", "")
        
        if item.is_dir():
            folder_name = domain_name if item.name == "models" else item.name
            new_target = target_root / folder_name
            scaffold_from_template(item, new_target, domain_name)
        else:
            target_root.mkdir(parents=True, exist_ok=True)
            target_file = target_root / clean_name
            
            if target_file.exists():
                click.echo(f"⏩ Skipping {target_file.name} (exists)")
                continue
            
            content = item.read_text(encoding="utf-8")
            target_file.write_text(content, encoding="utf-8")
            click.echo(f"✅ Created {target_file}")

@click.command()
@click.option('--ingest', is_flag=True, help="Initialize Ingest only.")
@click.option('--transform', is_flag=True, help="Initialize Transform only.")
def init(ingest, transform):
    """Initialize a Gojjam project with partitioned model directories."""
    
    init_all = not ingest and not transform
    target_dir = Path.cwd()
    
    click.secho("🏗️  Initializing Gojjam project...", fg="yellow", bold=True)

    try:
        base_templates = resources.files("gojjam.cli").joinpath("templates")

        if init_all or ingest:
            click.echo("📥 Scaffolding Ingest (Target folder: /ingest)...")
            ingest_templates = base_templates.joinpath("ingest")
            scaffold_from_template(ingest_templates, target_dir, "ingest")

        if init_all or transform:
            click.echo("🔄 Scaffolding Transform (Target folder: /transform)...")
            transform_templates = base_templates.joinpath("transform")
            scaffold_from_template(transform_templates, target_dir, "transform")

        click.secho("\n✨ Project initialized successfully!", fg="green", bold=True)

    except Exception as e:
        click.secho(f"\n❌ Initialization failed: {e}", fg="red", bold=True)
        raise click.Abort()
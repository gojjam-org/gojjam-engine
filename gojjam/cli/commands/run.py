
import click
from gojjam.ingest.main import GojjamIngestEngine
from gojjam.transform.main import GojjamTransformEngine

@click.command()
@click.option(
    '--sources', 
    default='./gojjam_ingest_sources.yml', 
    help='Path to the YAML file defining your extraction endpoints (APIs, DBs, CSVs).'
)
@click.option(
    '--sinks', 
    default='./gojjam_ingest_sinks.yml', 
    help='Path to the destination configuration where ingested data will be landed (e.g., Postgres, S3).'
)
@click.option(
    '--transform-cfg', 
    default='./gojjam_transform_settings.yml', 
    help='Path to the transformation manifest defining SQL models and execution logic.'
)
@click.option('--all', 'run_mode', flag_value='all', default=True, help='Run both Ingest and Transform (Default)')
@click.option('--ingest', 'run_mode', flag_value='ingest', help='Run only Gojjam-Ingest')
@click.option('--transform', 'run_mode', flag_value='transform', help='Run only Gojjam-Transform')
def run(sources, sinks, transform_cfg, run_mode):
    """Execute the data pipeline."""
    
    if run_mode in ['all', 'ingest']:
        click.secho("\n📥 [Stage 1/2] Starting Gojjam-Ingest...", fg="cyan", bold=True)
        try:
            runner = GojjamIngestEngine(datasource_path=sources, sink_path=sinks)
            runner.run_all()
            click.secho("✅ Ingestion complete.", fg="green")
        except Exception as e:
            click.secho(f"💥 Ingestion Failed: {e}", fg="red")
            if run_mode == 'all': return 

    if run_mode in ['all', 'transform']:
        click.secho("\n🔄 [Stage 2/2] Starting Gojjam-Transform...", fg="magenta", bold=True)
        try:
            t_engine = GojjamTransformEngine(trans_path=transform_cfg, sink_path=sinks)
            t_engine.deploy_and_register()
            t_engine.run_all()
            click.secho("✅ Transformation complete.", fg="green")
        except Exception as e:
            click.secho(f"💥 Transformation Failed: {e}", fg="red")

    click.secho("\n✨ Gojjam execution finished.", fg="white", bold=True)



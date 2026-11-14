import typer

from src.logging_conf import setup_logging
from src.processor import Processor
from src.sources.master import MASTER_REGISTRY

app = typer.Typer(
    name="minidata",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode=None,
    pretty_exceptions_enable=False,
)


@app.callback()
def cli() -> None:
    setup_logging()


@app.command()
def process() -> None:
    Processor(MASTER_REGISTRY).process()


def main() -> None:
    app()


if __name__ == "__main__":
    main()

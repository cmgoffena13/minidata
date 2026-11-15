from typer import Argument, Exit, Typer, echo

from src.exceptions import SourceNotFoundError
from src.logging_conf import setup_logging
from src.processor import Processor
from src.query import query as run_query
from src.sources.master import MASTER_REGISTRY

app = Typer(
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
def process(source: str | None = Argument(None)) -> None:
    try:
        Processor(MASTER_REGISTRY).process(source)
    except SourceNotFoundError as error:
        echo(str(error))
        raise Exit(1) from None


@app.command()
def source(pattern: str | None = Argument(None)) -> None:
    sources = MASTER_REGISTRY.get_sources()
    if pattern is not None:
        sources = MASTER_REGISTRY.find_sources(pattern)
        if not sources:
            echo(f'No matches for "{pattern}"')
            return
    for name in sorted(item.name for item in sources):
        echo(name)


@app.command("query")
def query_command(sql: str = Argument(...)) -> None:
    print(run_query(sql))


def main() -> None:
    app()


if __name__ == "__main__":
    main()

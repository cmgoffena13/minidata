import structlog

from src.exceptions import SourceNotFoundError
from src.pipeline.runner import PipelineRunner
from src.sources.registry import SourceRegistry

logger = structlog.getLogger(__name__)


class Processor:
    def __init__(self, master_registry: SourceRegistry):
        self.master_registry = master_registry

    def process(self, source_name: str | None = None) -> None:
        sources = self.master_registry.get_sources()
        if source_name is not None:
            sources = self.master_registry.find_sources(source_name)
            if not sources:
                raise SourceNotFoundError(f'Source "{source_name}" not found')
        logger.info("Processing started")
        for source_config in sources:
            PipelineRunner(source_config).run()

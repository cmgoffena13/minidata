import structlog

from src.pipeline.runner import PipelineRunner
from src.sources.registry import SourceRegistry

logger = structlog.getLogger(__name__)


class Processor:
    def __init__(self, master_registry: SourceRegistry):
        self.master_registry = master_registry

    def process(self) -> None:
        logger.info("Processing started")
        for source_config in self.master_registry.get_sources():
            PipelineRunner(source_config).run()

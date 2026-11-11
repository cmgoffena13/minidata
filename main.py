import structlog

from src.logging_conf import setup_logging
from src.processor import Processor
from src.sources.master import MASTER_REGISTRY

logger = structlog.getLogger("src")

if __name__ == "__main__":
    setup_logging()
    processor = Processor(MASTER_REGISTRY)
    processor.process()

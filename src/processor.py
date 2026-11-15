from concurrent.futures import ThreadPoolExecutor, as_completed

import structlog

from src.exceptions import SourceNotFoundError
from src.file_helper.base import BaseFileHelper
from src.file_helper.factory import create_file_helper
from src.pipeline.file_utils import staging_names_for_files
from src.pipeline.pipeline_utils import create_target_table
from src.pipeline.runner import PipelineRunner
from src.sources.base import SourceConfig
from src.sources.file_source import FileSourceConfig
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
            self._process_source(source_config)

    def _process_source(self, source_config: SourceConfig) -> None:
        if isinstance(source_config, FileSourceConfig):
            self._process_files(source_config)
        else:
            PipelineRunner(source_config).run()

    def _process_files(self, config: FileSourceConfig) -> None:
        create_target_table(config)
        helper = create_file_helper(config.directory, config.storage_options)
        helper.ensure_directory(config.archive_directory)
        paths = helper.scan(config.directory, config.pattern)
        names = staging_names_for_files(paths)
        errors: list[BaseException] = []
        with ThreadPoolExecutor() as pool:
            futures = [
                pool.submit(self._process_file, config, helper, path, names[path]) for path in paths
            ]
            for future in as_completed(futures):
                error = future.exception()
                if error is not None:
                    errors.append(error)
        if errors:
            raise errors[0]

    def _process_file(
        self,
        config: FileSourceConfig,
        helper: BaseFileHelper,
        path: str,
        staging_name: str,
    ) -> None:
        helper.archive(path, config.archive_directory)
        file_config = config.model_copy(update={"location": path, "staging_name": staging_name})
        PipelineRunner(file_config).run()
        helper.delete(path)

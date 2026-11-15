from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import structlog

from src.exceptions import ProcessingError, SourceNotFoundError
from src.file_helper.base import BaseFileHelper
from src.file_helper.factory import create_file_helper
from src.notify.slack import AlertLevel, SlackNotifier
from src.pipeline.file_utils import staging_names_for_files
from src.pipeline.pipeline_utils import create_target_table
from src.pipeline.runner import PipelineRunner, PipelineRunResult
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
        tasks = [
            task for source_config in sources for task in self._tasks_for_source(source_config)
        ]
        self._run_tasks(tasks)

    def _tasks_for_source(
        self, source_config: SourceConfig
    ) -> list[Callable[[], PipelineRunResult]]:
        tasks: list[Callable[[], PipelineRunResult]]
        if isinstance(source_config, FileSourceConfig):
            tasks = self._file_tasks(source_config)
        else:
            tasks = [lambda config=source_config: PipelineRunner(config).run()]
        return tasks

    def _file_tasks(self, config: FileSourceConfig) -> list[Callable[[], PipelineRunResult]]:
        create_target_table(config)
        helper = create_file_helper(config.directory, config.storage_options)
        helper.ensure_directory(config.archive_directory)
        paths = helper.scan(config.directory, config.pattern)
        names = staging_names_for_files(paths)
        return [
            lambda path=path, staging_name=names[path]: self._process_file(
                config, helper, path, staging_name
            )
            for path in paths
        ]

    def _run_tasks(self, tasks: list[Callable[[], PipelineRunResult]]) -> None:
        results: list[PipelineRunResult] = []
        with ThreadPoolExecutor() as pool:
            futures = [pool.submit(task) for task in tasks]
            for future in as_completed(futures):
                results.append(future.result())
        self._summarize_results(results)

    def _summarize_results(self, results: list[PipelineRunResult]) -> None:
        failures = [result for result in results if not result.success]
        success_count = len(results) - len(failures)
        logger.info(
            "Processing complete",
            successful=success_count,
            failed=len(failures),
        )
        if failures:
            failure_lines = [
                f"• {self._result_label(result)}: {result.error}" for result in failures
            ]
            message = "Failed:\n" + "\n".join(failure_lines)
            SlackNotifier(
                level=AlertLevel.ERROR,
                title="Pipeline Processing Summary",
                message=message,
                details={"successful": success_count, "failed": len(failures)},
            ).notify()
            raise ProcessingError(
                f"{len(failures)} pipeline task(s) failed:\n" + "\n".join(failure_lines),
                failures=[result.error for result in failures if result.error is not None],
            )

    def _result_label(self, result: PipelineRunResult) -> str:
        label = result.source_name
        if result.location is not None:
            label = f"{result.source_name} ({result.location})"
        return label

    def _process_file(
        self,
        config: FileSourceConfig,
        helper: BaseFileHelper,
        path: str,
        staging_name: str,
    ) -> PipelineRunResult:
        result: PipelineRunResult
        try:
            helper.archive(path, config.archive_directory)
            file_config = config.model_copy(update={"location": path, "staging_name": staging_name})
            result = PipelineRunner(file_config).run()
            if result.success:
                helper.delete(path)
        except Exception as caught:  # noqa: BLE001
            result = PipelineRunResult(
                success=False,
                source_name=config.name,
                error=caught,
                location=path,
            )
        return result

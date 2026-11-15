class SourceNotFoundError(ValueError):
    pass


class SchemaConformanceError(ValueError):
    pass


class ProcessingError(Exception):
    def __init__(self, message: str, failures: list[BaseException] | None = None) -> None:
        self.failures = failures or []
        super().__init__(message)


NON_RETRYABLE_EXCEPTIONS = (SchemaConformanceError,)

class SourceNotFoundError(ValueError):
    pass


class SchemaConformanceError(ValueError):
    pass


NON_RETRYABLE_EXCEPTIONS = (SchemaConformanceError,)

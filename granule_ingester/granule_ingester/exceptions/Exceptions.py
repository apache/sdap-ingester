class PipelineBuildingError(Exception):
    pass


class PipelineRunningError(Exception):
    pass


class TileProcessingError(PipelineRunningError):
    pass


class GranuleLoadingError(PipelineRunningError):
    pass


class LostConnectionError(Exception):
    pass


class RabbitMQLostConnectionError(LostConnectionError):
    pass


class CassandraLostConnectionError(LostConnectionError):
    pass


class SolrLostConnectionError(LostConnectionError):
    pass


class FailedHealthCheckError(Exception):
    pass


class CassandraFailedHealthCheckError(FailedHealthCheckError):
    pass


class SolrFailedHealthCheckError(FailedHealthCheckError):
    pass


class RabbitMQFailedHealthCheckError(FailedHealthCheckError):
    pass

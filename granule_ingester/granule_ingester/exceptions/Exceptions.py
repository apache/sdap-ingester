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


class ElasticsearchLostConnectionError(LostConnectionError):
    pass


class FailedHealthCheckError(Exception):
    pass


class CassandraFailedHealthCheckError(FailedHealthCheckError):
    pass


class SolrFailedHealthCheckError(FailedHealthCheckError):
    pass


class ElasticsearchFailedHealthCheckError(FailedHealthCheckError):
    pass


class RabbitMQFailedHealthCheckError(FailedHealthCheckError):
    pass

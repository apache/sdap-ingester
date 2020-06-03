import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConfigMap:
    def __init__(self, configmap_name, namespace, ingestion_order_store, output_collection='collections.yml'):
        self._ingestion_order_store = ingestion_order_store
        self._namespace = namespace
        self._configmap_name = configmap_name
        self._output_collection = output_collection
        config.load_kube_config()
        configuration = client.Configuration()
        self._api_instance = client.CoreV1Api(client.ApiClient(configuration))


    def __del__(self):
        pass

    def _create_configmap_object(self):

        metadata = client.V1ObjectMeta(
            name=self._configmap_name ,
            namespace=self._namespace,
        )
        
        data = {self._output_collection:self._ingestion_order_store.get_content()}

        configmap = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            data=data,
            metadata=metadata
        )
        return configmap

    def _get_deployed_config(self):
        try:
            api_list_response = self._api_instance.list_namespaced_config_map(self._namespace)
            config_keys = set()
            for item in api_list_response.items:
                config_keys = config_keys.union(item.data.keys())
        except ApiException as e:
            logger.error("Exception when calling Kubernetes CoreV1Api %s\n" % e)
        finally:
            return config_keys

    def publish(self):
        try:

            if self._output_collection in self._get_deployed_config():
                logger.info(f'replace configMap entry {self._output_collection}')
                api_response = self._api_instance.replace_namespaced_config_map(
                    name=self._output_collection,
                    namespace=self._namespace,
                    body=self._create_configmap_object()
                )
            else:
                logger.info(f'create configMap entry {self._output_collection}')
                api_response = self._api_instance.create_namespaced_config_map(
                    namespace=self._namespace,
                    body=self._create_configmap_object()
                )
            logger.info(api_response)

        except ApiException as e:
            logger.error("Exception when calling Kubernetes CoreV1Api %s\n" % e)


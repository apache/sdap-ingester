import os
import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from config_operator.config_source.exceptions import UnreadableFileException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class K8sConfigMap:
    def __init__(self, configmap_name, namespace, git_remote_config):
        self._git_remote_config = git_remote_config
        self._namespace = namespace
        self._configmap_name = configmap_name

        if os.getenv('KUBERNETES_SERVICE_HOST'):
            config.load_incluster_config()
        else:
            config.load_kube_config()
        configuration = client.Configuration()
        self._api_instance = client.ApiClient(configuration)
        self._api_core_v1_instance = client.CoreV1Api(self._api_instance)
        self.publish()

    def __del__(self):
        self._api_instance.close()

    def _create_configmap_object(self):

        metadata = client.V1ObjectMeta(
            name=self._configmap_name ,
            namespace=self._namespace,
        )

        data = {}        
        for f in self._git_remote_config.get_files():
            try:
                data[f] = self._git_remote_config.get_file_content(f)
            except UnreadableFileException as e:
                logger.error(f'file {f} cannot be read, ignored', e)
 
        configmap = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            data=data,
            metadata=metadata
        )
        return configmap

    def _get_deployed_config(self):
        """
        This method does not work in my test, the list of config available is not up to date
        """
        try:
            api_list_response = self._api_core_v1_instance.list_namespaced_config_map(self._namespace)
            config_keys = set()
            for item in api_list_response.items:
                config_keys = config_keys.union(item.data.keys())
        except ApiException as e:
            logger.error("Exception when calling Kubernetes CoreV1Api %s\n" % e)
        finally:
            return config_keys

    def _patch(self):
        """ replaces files available in the config but does not delete
            what is not available (e.g. which has not been parsed)"""
        try:
            logger.info(f'replace configMap entry {self._configmap_name}')
            api_response = self._api_core_v1_instance.patch_namespaced_config_map(
                name=self._configmap_name,
                namespace=self._namespace,
                body=self._create_configmap_object()
            )
            logger.info(api_response)
        except ApiException as e:
            raise e

    def _create(self):
        try:
            logger.info(f'create configMap entry {self._configmap_name}')
            api_response = self._api_core_v1_instance.create_namespaced_config_map(
                namespace=self._namespace,
                body=self._create_configmap_object()
            )
            logger.info(api_response)

        except ApiException as e:
            raise e

    def publish(self):
        try:
            self._create()
        except ApiException as e:
            logger.error("Exception when calling Kubernetes CoreV1Api ,create failed, try to replace %s\n" % e)
            self._patch()


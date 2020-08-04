import logging
import asyncio
import kopf
from config_operator.config_source import RemoteGitConfig
from config_operator.k8s import K8sConfigMap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_config_synchronizer(spec, namespace):
    if 'git-url' not in spec.keys():
        raise kopf.HandlerFatalError(f"git-url must be set.")
    if 'config-map' not in spec.keys():
        raise kopf.HandlerFatalError(f"config-map must be set.")

    git_url = spec['git-url']
    logger.info(f'git-url = {git_url}')
    config_map = spec['config-map']
    logger.info(f'config-map = {config_map}')

    _kwargs = {}
    for k in {'git-branch', 'git-username', 'git-token', 'update-every-seconds'}:
        if k in spec:
            logger.info(f'{k} = {spec[k]}')
            _kwargs[k.replace('-', '_')] = spec[k]

    config = RemoteGitConfig(git_url, **_kwargs)

    config_map = K8sConfigMap(config_map, namespace, config)

    asyncio.run(config.when_updated(config_map.publish))

    msg = f"configmap {config_map} created from git repo {git_url}"

    return msg


@kopf.on.create('sdap.apache.org', 'v1', 'gitbasedconfigs')
def create_fn(body, spec, **kwargs):
    logger.info(f'sdap config operator creation')

    namespace = body['metadata']['namespace']

    msg = create_config_synchronizer(spec, namespace)

    logger.info(f'sdap config operator created {msg}')

    return {'message': msg}


@kopf.on.update('sdap.apache.org', 'v1', 'gitbasedconfigs')
def update_fn(spec, status, namespace, **kwargs):
    logger.info(f'sdap config operator update')

    msg = create_config_synchronizer(spec, namespace)

    logger.info(f'sdap config operator updated {msg}')

    return {'message': msg}



@kopf.on.login()
def login_fn(**kwargs):
    return kopf.login_via_client(**kwargs)

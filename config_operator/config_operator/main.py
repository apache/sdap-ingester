import logging
import asyncio
import kopf
from config_operator.config_source import RemoteGitConfig
from config_operator.k8s import K8sConfigMap

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@kopf.on.create('sdap.apache.org', 'v1', 'gitbasedconfigs')
def create_fn(body, spec, **kwargs):
    # Get info from Git Repo Config object
    namespace = body['metadata']['namespace']

    if 'git-url' not in spec.keys():
        raise kopf.HandlerFatalError(f"git-url must be set.")
    if 'config-map' not in spec.keys():
        raise kopf.HandlerFatalError(f"config-map must be set.")

    git_url = spec['git-url']
    logger.info(f'git-url = {git_url}')
    config_map = spec['config-map']
    logger.info(f'config-map = {config_map}')

    _kargs = {}
    for k in {'git-branch', 'git-username', 'git-token', 'update-every-seconds'}:
        if k in spec:
            logger.info(f'{k} = {spec[k]}')
            _kargs[k.replace('-', '_')] = spec[k]

    config = RemoteGitConfig(git_url, **_kargs)

    config_map = K8sConfigMap(config_map, namespace, config)

    asyncio.run(config.when_updated(config_map.publish))

    msg = f"configmap {config_map} created from git repo {git_url}"
    return {'message': msg}


@kopf.on.login()
def login_fn(**kwargs):
    return kopf.login_via_client(**kwargs)

import kopf
from config_operator.config_source import RemoteGitConfig
from config_operator.k8s import K8sConfigMap


@kopf.on.create('sdap.apache.org', 'v1', 'git-repo-configs')
def create_fn(body, spec, **kwargs):
    # Get info from Git Repo Config object
    name = body['metadata']['name']
    namespace = body['metadata']['namespace']

    if 'git-url' not in spec.keys():
        raise kopf.HandlerFatalError(f"git-url must be set.")
    if 'config-map' not in spec.keys():
        raise kopf.HandlerFatalError(f"config-map must be set.")

    git_url = spec['git-url']
    config_map = spec['config-map']

    _kargs = {}
    for k in {'git-branch', 'git-token'}:
        if k in spec.keys():
            _kargs[k.split('-')[0]] = spec[k]

    config = RemoteGitConfig(git_url, **_kargs)

    config_map = K8sConfigMap(config_map, namespace, config)

    config.when_updated(config_map.publish)

    msg = f"configmap {config_map} created from git repo {git_url}"
    return {'message': msg}

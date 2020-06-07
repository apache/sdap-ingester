import argparse
from config_operator.config_source import RemoteGitConfig, LocalDirConfig
from config_operator.k8s import K8sConfigMap


def main():
    parser = argparse.ArgumentParser(description="Run git configuration synchronization operator, work on local-dir or git-url")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("-l", "--local-dir",
                             help="local directory where the configuration files are")
    input_group.add_argument("-gu", "--git-url",
                             help="git repository from which the configuration files are pulled/saved")
    parser.add_argument("-gb", "--git-branch", help="git branch from which the configuration files are pulled/saved",
                        default="master")
    parser.add_argument("-gl", "--git-local", help="local git repository", required=False)
    parser.add_argument("-gt", "--git-token", help="git personal access token used to access the repository")
    parser.add_argument("-n", "--namespace", help="kubernetes namespace where the configuration will be deployed", required=True)
    parser.add_argument("-cm", "--config-map", help="configmap name in kubernetes", required=True)

    parser.add_argument("-u", "--updated-continuously", nargs='?',  const=True, default=False,
                        help="k8 configMap is updated as soon as a syntactically correct configuration file is updated")

    options = parser.parse_args()

    if options.local_dir:
        config = LocalDirConfig(options.local_dir)
    else:
        config = RemoteGitConfig(options.git_url, branch=options.git_branch, token=options.git_token, local_dir=options.git_local)
    
    config_map = K8sConfigMap(options.config_map, options.namespace, config)
    config_map.publish()

    if options.updated_continuously:
        config.when_updated(config_map.publish)


if __name__ == "__main__":
    main()


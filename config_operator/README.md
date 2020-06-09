# Config Operator

## Purpose

Component which synchonizes local configuration in a directory, on a file system, or configuration files managed in a git repository with kubernetes configMap.
This helps to make a configuration managed by the operators in a single place (git, host file system) available in the kubernetes cluster.

For SDAP, it is used to make the configuration of the collections to be ingested available to the ingester service pods.

# Launch the service

The configurations can be updated while the service is running (-u). The configuration updates will be published to kubernetes pods by patching the existing configurations.

    config-operator -h
    config-operator -l tests/resources/data  -n sdap -cm collection-ingester-config
    config-operator --git-url=https://github.com/tloubrieu-jpl/sdap-ingester-config --namespace=sdap --config-map=collection-ingester-config

# Developers

    git clone ...
    cd config_operator
    pip install -e .
    pytest -d

# Containerizaion

## Docker

    docker build . -f containers/docker/Dockerfile --no-cache --tag tloubrieu/config-operator:latest
        
To publish the docker image on dockerhub do (step necessary for kubernetes deployment):

    docker login
    docker push tloubrieu/sdap-ingest-manager:latest
    
## Kubernetes
    
Deploy the gitbasedconfig operator:

     kubectl apply -f containers/k8s/config-operator-crd.yml -n sdap
     
Deploy the git custom resource which will be synchronize with a k8s configmap

     kubectl apply -f containers/k8s/git-repo-test.yml -n sdap
     
Check that the custom resource is deployed:

    kubectl get gitbasedconfigs -n sdap
    
Check that the configMap has been generated:

    kubectl get configmaps -n sdap
    

    
    
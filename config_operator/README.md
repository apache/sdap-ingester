# Config Operator

## Purpose

Component which synchonizes local configuration in a directory, on a file system, or configuration files managed in a git repository with kubernetes configMap.
This helps to make a configuration managed by the operators in a single place (git, host file system) available in the kubernetes cluster.

For SDAP, it is used to make the configuration of the collections to be ingested available to the ingester service pods.

The component runs as a kubernetes operator (see containerization section)

# Developers

    git clone ...
    cd config_operator
    pip install -e .
    pytest -d

# Containerization

## Docker

    docker build . -f containers/docker/Dockerfile -t nexusjpl/config-operator:latest
        
To publish the docker image on dockerhub do (step necessary for kubernetes deployment):

    docker login
    docker push nexusjpl/config-operator:latest
    
## Kubernetes

Delete pre-existing operator definitions:

    kubectl delete deployment.apps/git-repo-config-operator  -n sdap
   
Deploy the gitbasedconfig operator:

     kubectl apply -f containers/k8s/config-operator-crd.yml -n sdap
     
Deploy the git custom resource which will be synchronize with a k8s configmap

     kubectl apply -f containers/k8s/git-repo-test.yml -n sdap
     
Check that the custom resource is deployed:

    kubectl get gitbasedconfigs -n sdap
    
Check that the configMap has been generated:

    kubectl get configmaps -n sdap
    
Test an update of the config operator configuration:

    kubectl set image gitbasedconfig/nginx-deployment nginx=nginx:1.16.1 --record
    

    
    
# Jobs

This directory contains kubernetes deployments for all datasets ingested into PO.DAAC Nexus.

# Prerequisites

- Python 3.6+
- Access to create resources via `kubectl apply`

# How it works

Each dataset has a job configuration in the configs subdirectory. This job configuration is passed to the ningester code and is where the steps of the ingestion job are defined. The job configuration yaml is added to kubernetes as a config map.

## runjobs.py

The `runjobs.py` script is used to create kubernetes deployments for specific instances of ingestion jobs. There is one ingestion job for every granule in a dataset. The job template (`job-deployment-template.yml`) is used to create a kubernetes batch job deployment.

The job template is processed by the `runjobs.py` script and will produce one kubernetes job deployment file per granule to be ingested. The `runjobs.py` script uses the template and replaces the `$VARIABLE`s with values when creating the job deployment. The generated deployment files can than be run on kubernetes to ingest the granules.

`runjobs.py` can limit the number of jobs it tries to launch concurrently by using the `--max-concurrent-jobs` parameter. If set, only `--max-concurrent-jobs` will be started and they must all finish before the next `--max-concurrent-jobs` are started.

```
usage: runjobs.py [-h]
                  (-fp ~/data/smap/l2b/SMAP_L2B_SSS_\*.h5 | -flp ~/granules_to_ingest.txt | -fj | -tp ./temp/avhrr-oi)
                  -jc ./configs/smap-l2b.yml -jg smap-l2b
                  [-jdt ./job-deployment-template.yml]
                  [-c ./connection-config.yml] [-td /temp/sdap]
                  [-p dockermachost solr cassandra [dockermachost solr cassandra ...]]
                  [-nv 1.0.0-SNAPSHOT] [-ns default] [-mj 10] [-ds] [-pt]
                  [-dr] [-kt 60] [-vb]

Ingest one or more data granules using ningester on kubernetes.

optional arguments:
  -h, --help            show this help message and exit

Input Specification:
  Options for specifying where the source granules are read from. Exactly
  one option must be selected from this group.

  -fp ~/data/smap/l2b/SMAP_L2B_SSS_\*.h5, --filepath-pattern ~/data/smap/l2b/SMAP_L2B_SSS_\*.h5
                        The pattern used to match granule filenames for
                        ingestion. Make sure to properly escape bash
                        wildcards. This pattern is passed to python3
                        glob.iglob(filepath_pattern, recursive=True) (default:
                        None)
  -flp ~/granules_to_ingest.txt, --file-list-path ~/granules_to_ingest.txt
                        Path to a text file that contains a list of absolute
                        paths to granules that should be ingested, one path
                        per line. (default: None)
  -fj, --failed-jobs    Resubmit jobs where status is currently "Failed".
                        (default: False)
  -tp ./temp/avhrr-oi, --template-path ./temp/avhrr-oi
                        Path to a directory that contains fully resolved job
                        template files. All files in this directory will be
                        submitted as kubernetes jobs (default: None)

Required Arguments:
  Arguments that must be provided.

  -jc ./configs/smap-l2b.yml, --job-config ./configs/smap-l2b.yml
                        The file containing the configuration for the job. If
                        the --failed-jobs or --template-path option is used to
                        specify input, this argument is ignored. (default:
                        None)
  -jg smap-l2b, --job-group smap-l2b
                        The job group all jobs will be launched in. If the
                        --failed-jobs option is used to specify input, only
                        failed jobs in this job group will be resubmitted.
                        (default: None)

Configuration Files:
  Arguments that specify where various configuration files are located. By
  default, configuration files are assumed to be in the current working
  directory.

  -jdt ./job-deployment-template.yml, --job-deployment-template ./job-deployment-template.yml
                        The template used for the job deployment (default:
                        ./job-deployment-template.yml)
  -c ./connection-config.yml, --connection-settings ./connection-config.yml
                        The file containing deepdata-connection-config
                        configmap. (default: ./connection-config.yml)
  -td /temp/sdap, --temp-dir /temp/sdap
                        The temporary directory used to write out the resolved
                        job deployment files. (default: ./temp)

Template Substitution:
  Arguments that specify values that will be substitued in the job
  deployment template. These arguments will be ignored if using the
  --failed-jobs or --template-path option as input specification.

  -p dockermachost solr cassandra [dockermachost solr cassandra ...], --profiles dockermachost solr cassandra [dockermachost solr cassandra ...]
                        Profiles that should be active when launcing the job.
                        (default: ['deepdata', 'solr', 'cassandra'])
  -nv 1.0.0-SNAPSHOT, --ningester-version 1.0.0-SNAPSHOT
                        The version of the sdap/ningester docker image to use.
                        (default: 1.0.0-SNAPSHOT)

Runtime Behavior:
  Arguments that modify the behavior of how this tool runs.

  -ns default, --namespace default
                        The kubernetes namespace used when creating resources.
                        (default: default)
  -mj 10, --max-concurrent-jobs 10
                        The maximum number of jobs to launch at the same time.
                        (default: 10)
  -ds, --delete-successful
                        Use this flag to delete successful jobs before
                        submitting new ones. (default: False)
  -pt, --process-templates
                        Process job templates by doing key substitution but do
                        not run any kubectl commands. Prints commands it would
                        run. (default: False)
  -dr, --dry-run        Print the commands that would be run but do not
                        actually run them. (default: False)
  -kt 60, --kubectl-command-timeout 60
                        The maximum time to wait in seconds for kubectl commands to
                        complete. (default: 60)
  -vb, --verbose        Enable debug logging. (default: False)
```


## Job Template

The job template contains the actual job deployment yaml but with `$VARIABLE` placeholders in special locations. This is how we define the input to an individual run of a particular job. The variables that will be replaced are:  

- `JOBNAME`: The name of the job, must be unique. The value is automatically generated by taking the md5sum of the filename of the granule being ingested.
- `JOBGROUP`: The group this job belongs to. Provides an easy way of getting information about all jobs for a particular dataset.
- `GRANULEHOSTPATH`: The path on the host to the directory containing granules.
- `GRANULE`: The filename of the granule to be ingested.
- `CONFIGMAPNAME`: The name of the config map object for the job in kubernetes.
- `NINGESTERTAG`: The tag used to pull the sdap/ningester docker image.
- `PROFILES`: The profiles to activate when running the job.

## Connection configuration

The connection information is stored in a separate config map defined in `connection-config.yml`. This config map is shared as a volume with any new dataset ingestion job crated from the template. It contains a few different options for connecting depending on how the container is being run.

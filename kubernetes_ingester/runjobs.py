#!/usr/bin/env python3

import argparse
import datetime
import fileinput
import glob
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import signal
from enum import Enum
from pathlib import Path
from json import JSONDecodeError

KUBECTL_COMMAND_TIMEOUT = None
LOGGER = logging.getLogger('runjobs')


class CommandFailedException(Exception):
    pass


class OutputType(Enum):
    LOG = 1
    JSON = 2


def configure_logging():
    logformat = '%(asctime)s %(name)s:%(levelname)s [%(filename)s:%(lineno)d] %(message)s'
    level = logging.INFO
    datefmt = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=logformat, level=level, datefmt=datefmt, stream=sys.stdout)


def enable_verbose_logging(logger_name=None):
    logging.getLogger(logger_name).setLevel(logging.DEBUG)
    for handler in logging.getLogger(logger_name).handlers:
        handler.setLevel(logging.DEBUG)


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def run_piped_commands(*tuple_of_commands, output_type=OutputType.LOG, dry_run=True):
    command_as_string = ' | '.join([' '.join(command) for command in tuple_of_commands])

    LOGGER.debug("Running command: {}".format(command_as_string))

    if dry_run:
        return

    submitted_processes = []

    for command_index, command in enumerate(tuple_of_commands):
        if len(submitted_processes) == 0:
            submitted_processes += [subprocess.Popen(command, stdout=subprocess.PIPE)]
        else:
            submitted_processes += [subprocess.Popen(command, stdin=submitted_processes[command_index - 1].stdout,
                                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)]
            submitted_processes[command_index - 1].stdout.close()

    return wait_and_print(submitted_processes[-1], command_as_string, output_type=output_type)


def run_single_command_and_wait(command, output_type=OutputType.LOG, dry_run=True):
    command_as_string = ' '.join(command)
    LOGGER.debug("Running command: {}".format(command_as_string))

    if dry_run:
        return

    submitted_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return wait_and_print(submitted_process, command_as_string, output_type=output_type)


def wait_and_print(submitted_subprocess, command_string, output_type=OutputType.LOG):
    stdoutdata, stderrdata = submitted_subprocess.communicate(timeout=KUBECTL_COMMAND_TIMEOUT)
    stdout_as_list_of_strings = []
    if stdoutdata:
        stdout_as_list_of_strings = stdoutdata.decode(sys.getdefaultencoding()).strip().split('\n')

    if stderrdata:
        LOGGER.error(stderrdata.decode(sys.getdefaultencoding()).strip())

    if submitted_subprocess.returncode != 0:
        raise CommandFailedException("Command failed, check log messages for details. "
                                     "Command was: {}".format(command_string))

    if output_type == OutputType.LOG:
        LOGGER.info('\t\n'.join(stdout_as_list_of_strings))
        return None
    elif output_type == OutputType.JSON:
        try:
            json_ouput = json.loads('\n'.join(stdout_as_list_of_strings))
            # Always return output as a list of items. kubectl will return just the one object if the result
            # of the command is not a list.
            if 'items' not in json_ouput:
                items = [json_ouput]
                json_ouput = {'items': items}
            return json_ouput
        except JSONDecodeError as e:
            LOGGER.exception(
                "Could not decode output as json. Did you specify '-o json' for the last command in the pipe?")
            raise e


def get_completed_failed_and_running_jobs_names(namespace='default', job_group=None, job_names=tuple()):
    if job_group and job_names:
        raise RuntimeError("Cannot filter on both job_group and job_names at the same time. "
                           "Must choose one or the other")

    if job_group:
        jobgroup_filter = ['-l', 'jobgroup={}'.format(job_group)]
    else:
        jobgroup_filter = []

    if job_names:
        job_name_filter = [job_name for job_name in job_names]
    else:
        job_name_filter = []

    # kubectl get jobs -n default -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.conditions[?(@.type=="Failed")].status}{"\n"}{end}'
    get_failed_jobs_cmd = ['kubectl', 'get', 'jobs', '-n', namespace] \
                          + job_name_filter \
                          + jobgroup_filter \
                          + ['-o', 'json']

    job_list_json = run_single_command_and_wait(get_failed_jobs_cmd, output_type=OutputType.JSON, dry_run=False)
    job_list_json = job_list_json['items']

    failed_job_names = []
    complete_job_names = []
    running_job_names = []
    unknown_job_names = []

    for job_json in job_list_json:

        if ('conditions' in job_json['status']
                and any([(condition['type'] == "Failed" and condition['status'] == "True") for condition in
                         job_json['status']['conditions']])):
            failed_job_names.append(job_json['metadata']['name'])
        elif ('conditions' in job_json['status']
              and any([(condition['type'] == "Complete" and condition['status'] == "True") for condition in
                       job_json['status']['conditions']])):
            complete_job_names.append(job_json['metadata']['name'])
        elif ('active' in job_json['status']
              and job_json['status']['active'] == 1
              and job_json['metadata']['name'] not in failed_job_names
              and job_json['metadata']['name'] not in complete_job_names):
            running_job_names.append(job_json['metadata']['name'])
        else:
            unknown_job_names.append(job_json['metadata']['name'])

    return complete_job_names, failed_job_names, running_job_names, unknown_job_names


def delete_jobs(job_names, namespace='default', dry_run=True):
    for jobs in (job_names[pos:pos + 50] for pos in range(0, len(job_names), 50)):
        delete_job_cmd = ['kubectl', 'delete', 'job', '-n', namespace] + jobs

        run_single_command_and_wait(delete_job_cmd, output_type=OutputType.LOG, dry_run=dry_run)


def create_temp_job_json(temp_dir, job_names, namespace='default', dry_run=False):
    job_list_json = []

    for jobs in (job_names[pos:pos + 50] for pos in range(0, len(job_names), 50)):
        # kubectl get jobs -n sdap-uat mur-l4-sst-fda946bf07ed9678f1cb9fdcb50e4131 mur-l4-sst-bf07ed9678f1cb9fdcb50e4131... -o yaml
        get_job_json_cmd = ['kubectl', 'get', 'jobs', '-n', namespace] + jobs + ['-o', 'json']
        jobs_list = run_single_command_and_wait(get_job_json_cmd, output_type=OutputType.JSON, dry_run=False)
        job_list_json += jobs_list['items']

    job_json_file_paths = []
    for job_json in job_list_json:
        del job_json['status']
        del job_json['metadata']['annotations']['kubectl.kubernetes.io/last-applied-configuration']
        del job_json['metadata']['creationTimestamp']
        del job_json['metadata']['namespace']
        del job_json['metadata']['resourceVersion']
        del job_json['metadata']['selfLink']
        del job_json['metadata']['uid']
        del job_json['spec']['completions']
        del job_json['spec']['parallelism']
        del job_json['spec']['selector']
        del job_json['spec']['template']['metadata']['labels']['controller-uid']

        job_json_path = os.path.join(temp_dir, '{}.json'.format(job_json['metadata']['name']))
        if dry_run:
            LOGGER.info("touch {}".format(job_json_path))
        else:
            with open(job_json_path, 'w') as job_json_file:
                job_json_file.write(json.dumps(job_json))
            assert is_non_zero_file(job_json_path), "Failed to create temp job json file {}".format(job_json_path)

        job_json_file_paths += [job_json_path]

    return job_json_file_paths


def create_and_run_jobs(args):
    # Wipe out previously created job templates.
    temp_dir = os.path.join(args.temp_dir, args.job_group)
    namespace = args.namespace
    dry_run = args.dry_run

    if dry_run:
        LOGGER.info("mkdir -p {}".format(temp_dir))
    else:
        os.makedirs(temp_dir, exist_ok=True)

    job_files = []
    files = []
    if args.filepath_pattern:
        # Glob the files to be ingested
        files = glob.iglob(args.filepath_pattern, recursive=True)
    elif args.file_list_path:
        # Parse filepaths from input
        with open(args.file_list_path, 'r') as granule_list:
            files = granule_list.readlines()
    elif args.failed_jobs:
        # Resubmit failed jobs
        completed_job_names, failed_job_names, running_job_names, unknown_job_names = get_completed_failed_and_running_jobs_names(
            namespace=namespace,
            job_group=args.job_group)
        if not failed_job_names:
            LOGGER.warning("No failed jobs found with job group {}. Nothing to do!".format(args.job_group))
            return
        job_files = create_temp_job_json(temp_dir, failed_job_names, namespace=namespace,
                                         dry_run=dry_run)
        delete_jobs(failed_job_names, namespace=namespace, dry_run=(dry_run or args.process_templates))
    elif args.template_path:
        # Submit resolved job templates directly
        job_files = [os.path.join(args.template_path, filename) for filename in os.listdir(args.template_path)]
        job_names = [os.path.splitext(os.path.basename(the_file))[0] for the_file in job_files]
        try:
            delete_jobs(job_names, namespace=namespace, dry_run=(dry_run or args.process_templates))
        except CommandFailedException:
            LOGGER.warning("Failed to delete jobs, assuming they do not exist. Continuing execution.")

    else:
        raise Exception("Unknown input")

    # Config map names are just the filename minus extension
    job_config_map_name = os.path.splitext(os.path.basename(args.job_config))[0]
    connection_config_map_name = os.path.splitext(os.path.basename(args.connection_settings))[0]

    # For every file to be ingested, create a deployment by replacing the placeholders in the template with actual values
    for the_file in files:
        filename = os.path.basename(the_file)
        filepath = os.path.dirname(the_file)
        md5sum = hashlib.md5(filename.encode()).hexdigest()
        granule_job_filepath = os.path.join(temp_dir, '{}-{}.yml'.format(args.job_group, md5sum))
        job_files += [granule_job_filepath]

        if dry_run:
            LOGGER.info("cp {} {}".format(args.job_deployment_template, granule_job_filepath))
            LOGGER.info("$JOBNAME={}".format(md5sum))
            LOGGER.info("$JOBGROUP={}".format(args.job_group))
            LOGGER.info("$GRANULEHOSTPATH={}".format(filepath))
            LOGGER.info("$GRANULE={}".format(filename))
            LOGGER.info("$JOBCONFIGMAPNAME={}".format(job_config_map_name))
            LOGGER.info("$CONNECTIONCONFIGMAPNAME={}".format(connection_config_map_name))
            LOGGER.info("$NINGESTERTAG={}".format(args.ningester_version))
            LOGGER.info("$PROFILES={}".format(','.join(args.profiles)))
        else:
            shutil.copy(args.job_deployment_template, granule_job_filepath)
            for line in fileinput.input(granule_job_filepath, inplace=True):
                line = line.replace('$JOBNAME', md5sum)
                line = line.replace('$JOBGROUP', args.job_group)
                line = line.replace('$GRANULEHOSTPATH', filepath)
                line = line.replace('$GRANULE', filename)
                line = line.replace('$JOBCONFIGMAPNAME', job_config_map_name)
                line = line.replace('$CONNECTIONCONFIGMAPNAME', connection_config_map_name)
                line = line.replace('$NINGESTERTAG', args.ningester_version)
                line = line.replace('$PROFILES', ','.join(args.profiles))

                print(line, end='')

    # Generate a configmap from the connection settings and apply it
    connection_settings = os.path.join(os.path.join(Path(__file__).parent.absolute(), args.connection_settings))
    generate_connection_config_map = ['kubectl', 'create', 'configmap', connection_config_map_name, '--from-file',
                                      connection_settings, '--dry-run', '-o', 'json']
    kubectl_apply_from_stdin = ['kubectl', 'apply', '-n', namespace, '-f', '-']
    run_piped_commands(generate_connection_config_map, kubectl_apply_from_stdin,
                       dry_run=(dry_run or args.process_templates))

    # Generate a configmap from the job configruation and apply it only if we're not rerunning failed jobs
    if not (args.failed_jobs or args.template_path):
        job_config = os.path.join(os.path.join(Path(__file__).parent.absolute(), args.job_config))
        generate_job_config_map = ['kubectl', 'create', 'configmap', job_config_map_name, '--from-file',
                                   job_config, '--dry-run', '-o', 'json']
        run_piped_commands(generate_job_config_map, kubectl_apply_from_stdin,
                           dry_run=(dry_run or args.process_templates))

    # Submit all the jobs to the kubernetes cluster but only submit `args.max_concurrent_jobs` at any given time.
    # Wait for all jobs to complete before submitting the next `args.max_concurrent_jobs` jobs
    max_concurrent_jobs = int(args.max_concurrent_jobs)
    total_jobs = len(job_files)
    total_success = 0
    total_fail = 0
    for i in range(0, total_jobs, max_concurrent_jobs):
        chunk = job_files[i:i + max_concurrent_jobs]

        chunk = [e for l in zip(['-f'] * len(chunk), chunk) for e in l]
        job_names_in_chunk = tuple([os.path.splitext(os.path.basename(the_file))[0] for the_file in chunk[1::2]])

        apply_job_deployments = ['kubectl', 'apply', '-n', namespace, *chunk]

        LOGGER.info("Launching {} more job(s)".format(len(chunk) // 2))
        run_single_command_and_wait(apply_job_deployments, dry_run=(dry_run or args.process_templates))

        if not (dry_run or args.process_templates):
            completed_job_names, failed_job_names, running_job_names, unknown_job_names = get_completed_failed_and_running_jobs_names(
                namespace=namespace,
                job_names=job_names_in_chunk)

            while len(completed_job_names) + len(failed_job_names) < len(chunk) // 2:
                LOGGER.info(
                    "{} unknown, {} running, {} success, {} failed, {} finished out of {} total".format(
                        len(unknown_job_names),
                        len(running_job_names),
                        total_success + len(completed_job_names),
                        total_fail + len(failed_job_names),
                        total_success + len(
                            completed_job_names) + total_fail + len(
                            failed_job_names), total_jobs))
                time.sleep(15)
                completed_job_names, failed_job_names, running_job_names, unknown_job_names = get_completed_failed_and_running_jobs_names(
                    namespace=namespace,
                    job_names=job_names_in_chunk)

            total_success += len(completed_job_names)
            total_fail += len(failed_job_names)

            LOGGER.info("{} unknown, {} running, {} complete, {} failed, {} finished out of {} total".format(
                len(unknown_job_names),
                len(running_job_names),
                total_success, total_fail,
                total_success + total_fail,
                total_jobs))

            if args.delete_successful:
                LOGGER.info("Deleting successful jobs")
                # Get jobs in group as JSON output
                completed_job_names, failed_job_names, running_job_names, unknown_job_names = get_completed_failed_and_running_jobs_names(
                    namespace=namespace,
                    job_names=job_names_in_chunk)

                # If some jobs are complete, delete them
                if completed_job_names:
                    delete_jobs(completed_job_names, namespace=namespace, dry_run=dry_run)
                    # Also remove the resolved template
                    job_files_in_chunk = [the_file for the_file in chunk[1::2]]
                    for job_file_path in job_files_in_chunk:
                        if os.path.splitext(os.path.basename(job_file_path))[0] in completed_job_names:
                            os.remove(job_file_path)


def parse_args():
    parser = argparse.ArgumentParser(description='Ingest one or more data granules using ningester on kubernetes.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Group arguments for how the program should look for granules to ingest
    input_group = parser.add_argument_group('Input Specification',
                                            'Options for specifying where the source granules are read from. '
                                            'Exactly one option must be selected from this group.')
    input_group_mutex = input_group.add_mutually_exclusive_group(required=True)
    input_group_mutex.add_argument('-fp', '--filepath-pattern',
                                   help='The pattern used to match granule filenames for ingestion. '
                                        'Make sure to properly escape bash wildcards. '
                                        'This pattern is passed to python3 glob.iglob(filepath_pattern, recursive=True)',
                                   metavar='~/data/smap/l2b/SMAP_L2B_SSS_\*.h5')

    input_group_mutex.add_argument('-flp', '--file-list-path',
                                   help='Path to a text file that contains a list of absolute paths to '
                                        'granules that should be ingested, one path per line.',
                                   metavar='~/granules_to_ingest.txt')

    input_group_mutex.add_argument('-fj', '--failed-jobs',
                                   help='Resubmit jobs where status is currently "Failed".',
                                   action='store_true')

    input_group_mutex.add_argument('-tp', '--template-path',
                                   help='Path to a directory that contains fully resolved job template files. All files'
                                        ' in this directory will be submitted as kubernetes jobs',
                                   metavar='./temp/avhrr-oi')

    # End Group

    # Group required arguments
    required_args_group = parser.add_argument_group('Required Arguments', 'Arguments that must be provided.')
    required_args_group.add_argument('-jc', '--job-config',
                                     help='The file containing the configuration for the job. '
                                          'If the --failed-jobs or --template-path option is used to specify input, '
                                          'this argument is ignored.',
                                     required=True,
                                     metavar='./configs/smap-l2b.yml')

    required_args_group.add_argument('-jg', '--job-group',
                                     help='The job group all jobs will be launched in. '
                                          'If the --failed-jobs option is used to specify input, '
                                          'only failed jobs in this job group will be resubmitted.',
                                     required=True,
                                     metavar='smap-l2b')
    # End Group

    # Group config file arguments
    config_files_group = parser.add_argument_group('Configuration Files',
                                                   'Arguments that specify where various configuration files are '
                                                   'located. By default, configuration files are assumed to '
                                                   'be in the current working directory.')
    config_files_group.add_argument('-jdt', '--job-deployment-template',
                                    help='The template used for the job deployment',
                                    required=False,
                                    default='./resources/job-deployment-template.yml',
                                    metavar='./resources/job-deployment-template.yml')

    config_files_group.add_argument('-c', '--connection-settings',
                                    help='The file containing deepdata-connection-config configmap.',
                                    required=False,
                                    default='./resources/connection-config.yml',
                                    metavar='./resources/connection-config.yml')

    config_files_group.add_argument('-td', '--temp-dir',
                                    help='The temporary directory used to write out the resolved job deployment files.',
                                    required=False,
                                    default='./temp',
                                    metavar='/temp/sdap')
    # End Group

    # Template substitution group
    template_substitution_group = parser.add_argument_group('Template Substitution',
                                                            'Arguments that specify values that will be '
                                                            'substitued in the job deployment template. '
                                                            'These arguments will be ignored if using the '
                                                            '--failed-jobs or --template-path option as input '
                                                            'specification.')
    template_substitution_group.add_argument('-p', '--profiles',
                                             help='Profiles that should be active when launcing the job.',
                                             required=False,
                                             nargs='+',
                                             default=['deepdata', 'solr', 'cassandra'],
                                             metavar="dockermachost solr cassandra")

    template_substitution_group.add_argument('-nv', '--ningester-version',
                                             help='The version of the sdap/ningester docker image to use.',
                                             required=False,
                                             default='1.0.0-SNAPSHOT',
                                             metavar='1.0.0-SNAPSHOT')
    # End Group

    # Runtime behavior group
    runtime_behavior_group = parser.add_argument_group('Runtime Behavior',
                                                       'Arguments that modify the behavior of how this tool runs.')
    runtime_behavior_group.add_argument('-ns', '--namespace',
                                        help='The kubernetes namespace used when creating resources.',
                                        required=False,
                                        default='default',
                                        metavar='default')

    runtime_behavior_group.add_argument('-mj', '--max-concurrent-jobs',
                                        help='The maximum number of jobs to launch at the same time.',
                                        required=False,
                                        default=10,
                                        metavar='10')

    runtime_behavior_group.add_argument('-ds', '--delete-successful',
                                        help='Use this flag to delete successful jobs before submitting new ones.',
                                        required=False,
                                        action='store_true')

    runtime_behavior_group.add_argument('-pt', '--process-templates',
                                        help='Process job templates by doing key substitution but do not run any '
                                             'kubectl commands. Prints commands it would run.',
                                        required=False,
                                        action='store_true')

    runtime_behavior_group.add_argument('-dr', '--dry-run',
                                        help='Print the commands that would be run but do not actually run them.',
                                        required=False,
                                        action='store_true')

    runtime_behavior_group.add_argument('-kt', '--kubectl-command-timeout',
                                        help='The maximum time to wait for kubectl commands to complete.',
                                        required=False,
                                        default=60,
                                        metavar='60')

    runtime_behavior_group.add_argument('-vb', '--verbose',
                                        help='Enable debug logging.',
                                        required=False,
                                        action='store_true')
    # End Group

    return parser.parse_args()


def interrupt(sig, frame):

    LOGGER.warning("Received signal {}. {}".format(sig, frame))
    sys.exit(sig)


def run_granule_as_kubernetes_pod():
    configure_logging()

    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGHUP, interrupt)
    signal.signal(signal.SIGQUIT, interrupt)

    LOGGER.info("Starting {}".format(datetime.datetime.now()))
    the_args = parse_args()
    LOGGER.info("Ran with arguments {}".format(the_args))
    KUBECTL_COMMAND_TIMEOUT = the_args.kubectl_command_timeout

    if the_args.verbose:
        enable_verbose_logging(LOGGER.name)

    exit_code = 0
    try:
        create_and_run_jobs(the_args)
    except Exception:
        LOGGER.exception("Encountered an unexpected error.")
        exit_code = 1

    LOGGER.info("Finished {}".format(datetime.datetime.now()))
    sys.exit(exit_code)

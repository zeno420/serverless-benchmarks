#!python3


import argparse
import datetime
import importlib
import json
import sys
import traceback

from experiments_utils import *
from cache import cache
from CodePackage import CodePackage
from CloudExperiments import ExperimentRunner

# TODO: replace with something more sustainable
sys.path.append(PROJECT_DIR)

parser = argparse.ArgumentParser(description='Run cloud experiments.')
parser.add_argument('action', choices=['publish', 'test_invoke', 'experiment', 'logs'],
                    help='Benchmark name')
parser.add_argument('benchmark', type=str, help='Benchmark name')
parser.add_argument('output_dir', type=str, help='Output dir')
parser.add_argument('size', choices=['test', 'small', 'large'],
                    help='Benchmark input test size')
parser.add_argument('config', type=str, help='Config JSON for experiments')
parser.add_argument('--deployment', choices=['azure', 'aws', 'local'],
                    help='Cloud to use')
parser.add_argument('--experiment', choices=['time_warm'],
                    help='Experiment to run')
parser.add_argument('--language', choices=['python', 'nodejs', 'cpp'],
                    default=None, help='Benchmark language')
parser.add_argument('--repetitions', action='store', default=5, type=int,
                    help='Number of experimental repetitions')
parser.add_argument('--cache', action='store', default='cache', type=str,
                    help='Cache directory')
parser.add_argument('--function-name', action='store', default='', type=str,
                    help='Override function name for random generation.')
parser.add_argument('--update', action='store_true', default=False,
                    help='Update function code in cache and deployment.')
parser.add_argument('--update-storage', action='store_true', default=False,
                    help='Update storage files in deployment.')
parser.add_argument('--preserve-out', action='store_true', default=False,
                    help='Dont clean output directory [default false]')
parser.add_argument('--verbose', action='store', default=False, type=bool,
                    help='Verbose output')
args = parser.parse_args()

# -1. Get provider config and create cloud object
default_config = json.load(open(args.config, 'r'))
systems_config = json.load(open(os.path.join(PROJECT_DIR, 'config', 'systems.json'), 'r'))
output_dir = None

cache_client = cache(args.cache)
docker_client = docker.from_env()
deployment_client = None

experiment_config = {
    'experiments': {}
}

# CLI overrides JSON options
# Language
if args.language:
    language = args.language
else:
    language = default_config['experiments']['language']
experiment_config['experiments']['language'] = language

if args.deployment:
    deployment = args.deployment
else:
    deployment = default_config['experiments']['deployment']
experiment_config['experiments']['deployment'] = {
    'name': deployment,
    'config': default_config[deployment]
}
experiment_config['experiments']['update_code'] = args.update
experiment_config['experiments']['update_storage'] = args.update_storage
experiment_config['experiments']['benchmark'] = args.benchmark

try:
    benchmark_summary = {}
    if language not in default_config[deployment]['runtime']:
        raise RuntimeError('Language {} is not supported on cloud {}'.format(language, args.deployment))
    experiment_config['experiments']['runtime'] = default_config[deployment]['runtime'][language]
    experiment_config['experiments']['region'] = default_config[deployment]['region']
    # Load cached secrets
    cached_config = cache_client.get_config(deployment)
    if cached_config is not None:
        experiment_config['experiments']['deployment'].update(cached_config)
    # Create deployment client
    if deployment == 'aws':
        from cloud_frontend.aws import aws
        deployment_client = aws.aws(cache_client, experiment_config['experiments']['deployment'],
                language, docker_client)
    else:
        from cloud_frontend.azure import azure
        deployment_client = azure.azure(cache_client, experiment_config['experiments']['deployment'],
                language, docker_client)

    # 0. Input args
    args = parser.parse_args()
    verbose = args.verbose

    # 1. Create output dir
    output_dir = create_output(args.output_dir, args.preserve_out, args.verbose)
    logging.info('Created experiment output at {}'.format(args.output_dir))

    if args.action == 'publish':
        package = CodePackage(args.benchmark, experiment_config, output_dir,
                systems_config[deployment], cache_client, docker_client, args.update)
        func = deployment_client.create_function(package, experiment_config)
    elif args.action == 'test_invoke':
        package = CodePackage(args.benchmark, experiment_config, output_dir,
                systems_config[deployment], cache_client, docker_client, args.update)
        # 5. Prepare benchmark input
        input_config = prepare_input(
            client=deployment_client,
            benchmark=args.benchmark,
            size=args.size,
            update_storage=experiment_config['experiments']['update_storage']
        )
        func = deployment_client.create_function(package, experiment_config)

        # TODO bucket save of results
        bucket = None
        #bucket = deployment_client.prepare_experiment(args.benchmark)
        #input_config['logs'] = { 'bucket': bucket }

        begin = datetime.datetime.now()
        ret = deployment_client.invoke_sync(func, input_config)
        print(ret)
        benchmark_summary['experiment'] = {
            'function_name': func,
            'begin': float(begin.strftime('%s.%f')),
            'invocations': 1
        }
        if bucket:
            ret['results_bucket'] = bucket
        benchmark_summary['config'] = experiment_config
        with open('experiments.json', 'w') as out_f:
            json.dump(benchmark_summary, out_f, indent=2)
    elif args.action == 'experiment':
        # Prepare benchmark input
        input_config = prepare_input(
            client=deployment_client,
            benchmark=args.benchmark,
            size=args.size,
            update_storage=experiment_config['experiments']['update_storage']
        )
        package = CodePackage(args.benchmark, experiment_config, output_dir,
                systems_config[deployment], cache_client, docker_client, args.update)
        runner = ExperimentRunner(
            benchmark=args.benchmark,
            output_dir=output_dir,
            language=language,
            input_config=input_config,
            experiment_config=experiment_config,
            code_package=package,
            deployment_client=deployment_client,
            cache_client=cache_client
        )
    else:
        pass

    ## 2. Locate benchmark
    #benchmark_path = find_benchmark(args.benchmark, 'benchmarks')
    #if benchmark_path is None:
    #    raise RuntimeError('Benchmark {} not found in {}!'.format(args.benchmark, benchmarks_dir))
    #logging.info('Located benchmark {} at {}'.format(args.benchmark, benchmark_path))


    # 6. Create function if it does not exist
    #func, code_size = deployment_client.create_function(args.benchmark,
    #        benchmark_path, experiment_config, args.function_name)

    # 7. Invoke!


except Exception as e:
    print(e)
    traceback.print_exc()
    if output_dir:
        print('# Experiments failed! See {}/out.log for details'.format(output_dir))
finally:
    # Close
    if deployment_client is not None:
        deployment_client.shutdown()
    cache_client.shutdown()

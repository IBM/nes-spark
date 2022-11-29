import subprocess
import json
from datetime import datetime
import os
import time 
import boto3
import re
import networkx as nx
import sys
import matplotlib.pyplot as plt
import pandas as pd
import statistics

#NOTE: Set correct logs folder path in spark configuration file spark-defaults.conf before running

DEBUG = False

# Define benchmark name (either tpch or tpcds-sql
BENCHMARK_NAME = 'tpch'

# Define number of iterations
NUM_ITERATIONS = 5

# Define list of number of executors
EXECUTORS = [ 1,
    10,
    20,
    30,
    40,
    50
]

# Define list of input data sizes
DATA_SIZE = [ 100
]

# Define list of queries to consider  
QUERIES= [
    "q9"
#    "q14a",
#    "q67"
#    "q72"
#    "q95"
#    "q23a"
]

# Define history server address
history_server = ''

# Define global variable for the reference job stages to use when changing the number of executors
ref_info = []

# Define if to consider dynamic allocation 
dynamic = False

def get_info_from_history_server(request):
    result = subprocess.run(['curl', '-s', history_server + request], capture_output=True, text=True)
    content = result.stdout
    json_data = json.loads(content)
    return json_data

def get_list_applications_ids():
    request = 'applications/'
    json_data = get_info_from_history_server(request)
    list_apps = []
    for i in range(len(json_data)):
        list_apps.append(json_data[i]['id'])
    return list_apps

def get_list_jobs_with_stages(app_id):
    app_request = 'applications/' + app_id 
    jobs_request = app_request + '/jobs/'
    jobs_json_data = get_info_from_history_server(jobs_request)
    list_jobs = []
    list_keys = []
    list_ids = []
    for idx_job in range(len(jobs_json_data)):
        temp_stages_ids = jobs_json_data[idx_job]['stageIds']
        stages_ids = []
        for stage_id in temp_stages_ids:
            stage_request = app_request + '/stages/' + str(stage_id)
            stage_json_data = get_info_from_history_server(stage_request)[0]
            if stage_json_data['status'] == 'COMPLETE':
                stages_ids.append(stage_id)
                list_ids.append(stage_id)
                list_keys.append(stage_json_data['stageKey'])
        list_jobs.append([jobs_json_data[idx_job]['jobId'], stages_ids])
    if len(ref_info) == 0:
        print("Stage Ids: {}".format(list_ids))
        print("Stage Keys: {}".format(list_keys))
    return list_jobs

def get_diff_timestamps(json_data):
    first_task_ts = datetime.strptime(json_data['firstTaskLaunchedTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')
    compl_ts = datetime.strptime(json_data['completionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')
    return (compl_ts - first_task_ts).total_seconds(), first_task_ts
  
def get_avg_exs_time(apps_ids, s3_client, query, ds, min_exs, max_exs):
    bucket = os.getenv('MINIO_LOGS_BUCKET').split('/')[0]
    if min_exs != max_exs:
        prefix = 'logs/' + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-dyn' + str(min_exs) + 'to' + str(max_exs) + '-test'
    else:
        prefix = 'logs/' + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-ex' + str(min_exs) + '-test'
    apps_exs_times = []
    for app_idx, app_id in enumerate(apps_ids):
        # Get the application event logs from MinIO
        s3_client.download_file(bucket, prefix + '/' + app_id, app_id)
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerExecutorAdded\") | .\"Timestamp\"\'"
        exs_start = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')[:-1]
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerExecutorAdded\") | .\"Executor ID\"\'"
        exs_start_ids = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')[:-1]
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerExecutorRemoved\") | .\"Timestamp\"\'"
        exs_end = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')[:-1]
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerExecutorRemoved\") | .\"Executor ID\"\'"
        exs_end_ids = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')[:-1]
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerApplicationEnd\") | .\"Timestamp\"\'"
        app_end = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
        os.remove(app_id)
        total_exs_time = 0
        for ex_start, ex_id in zip(exs_start, exs_start_ids):
            if ex_id in exs_end_ids:
                total_exs_time += float(exs_end[exs_end_ids.index(ex_id)]) - float(ex_start)
            else:
                total_exs_time += float(app_end) - float(ex_start)
        apps_exs_times.append(total_exs_time)
    return [sum(apps_exs_times)/len(apps_ids), statistics.median(apps_exs_times), min(apps_exs_times), max(apps_exs_times)]

def filter_logs(apps_ids, apps_jobs_ids):
    apps_times = []
    jobs_times = [[] for x in range(len(apps_jobs_ids[0]))]
    stages_times = [[] for x in range(len(apps_jobs_ids[0]))]
    for idx_app in range(NUM_ITERATIONS):
        # Store current run application runtime
        app_id = apps_ids[idx_app]
        app_request = 'applications/' + app_id 
        json_data = get_info_from_history_server(app_request)
        apps_times.append(json_data['attempts'][0]['duration']/1000)
        for idx_job, job in enumerate(apps_jobs_ids[idx_app]):
            temp_list = []
            set_min = False
            min_first_task_ts = datetime.min
            for idx_stage, stage in enumerate(job[1]):
                # Store current stage runtime
                stage_request = app_request + '/stages/' + str(stage)
                json_data = get_info_from_history_server(stage_request)[0]
                if json_data['status'] == 'COMPLETE':
                    # Consider only completed stages
                    stage_time, first_task_ts = get_diff_timestamps(json_data)
                    if not set_min:
                        min_first_task_ts = first_task_ts
                        set_min = True
                    tasks_acc_time = sum([float(json_data['tasks'][task_id]['duration']/1000) for task_id in json_data['tasks']])
                    temp_list.append([stage_time, tasks_acc_time])
                    if first_task_ts < min_first_task_ts:
                        min_first_task_ts = first_task_ts
            stages_times[idx_job].append(temp_list)
            # Store current run jobs runtime
            job_request = app_request + '/jobs/' + str(job[0])
            json_data = get_info_from_history_server(job_request)
            job_compl_ts = datetime.strptime(json_data['completionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')
            jobs_times[idx_job].append((job_compl_ts - min_first_task_ts).total_seconds())
    return apps_times, jobs_times, stages_times

def compute_statistics(apps_times, jobs_times, stages_times):
    # Input stages_times list format: [#jobs][#iterations][#StagesXjob][2]
    # Output list format: [ [app_stats (sec)], [[[job0_stats (sec)], [[stage0_avg (sec), tasks_acc_avg (sec), stage0_med (sec), stage_0_min (sec), stage0_max(max)],..]],..]]
    app_avg = sum(apps_times)/NUM_ITERATIONS
    app_median = statistics.median(apps_times)
    app_min = min(apps_times)
    app_max = max(apps_times)
    app_stats = [app_avg, app_median, app_min, app_max]
    jobs_avg = [sum(job_times)/NUM_ITERATIONS for job_times in jobs_times]
    jobs_median = [statistics.median(job_times) for job_times in jobs_times]
    jobs_min = [min(job_times) for job_times in jobs_times] 
    jobs_max = [max(job_times) for job_times in jobs_times] 
    stages_stats = []
    for job_stages in stages_times:
        temp_list = []
        for idx_stage in range(len(job_stages[0])):
            stage_time_sum = 0
            tasks_acc_time_sum = 0
            list_times = []
            for it in range(NUM_ITERATIONS):
                stage_time_sum += job_stages[it][idx_stage][0]
                list_times.append(job_stages[it][idx_stage][0])
                tasks_acc_time_sum += job_stages[it][idx_stage][1]
            temp_list.append([stage_time_sum/NUM_ITERATIONS, tasks_acc_time_sum/NUM_ITERATIONS, statistics.median(list_times), min(list_times), max(list_times)])
        stages_stats.append(temp_list)
    temp_list = []
    for i in range(len(jobs_avg)):
        temp_list.append([[jobs_avg[i], jobs_median[i], jobs_min[i], jobs_max[i]], stages_stats[i]])
    return [app_stats, temp_list]
 
def get_statistics(apps_ids, apps_jobs_ids):
    # Read logs and derive applications, jobs, stages, and tasks accumulated times over NUM_ITERATIONS
    apps_times, jobs_times, stages_times = filter_logs(apps_ids, apps_jobs_ids)
    stats = compute_statistics(apps_times, jobs_times, stages_times)
    return stats

def get_apps_jobs_with_stages(apps_ids):
    # Get list of jobs with relative stages 
    apps_jobs_ids = []
    for app_id in apps_ids:
        app_jobs_with_stages = get_list_jobs_with_stages(app_id)
        app_jobs_with_stages.reverse()
        apps_jobs_ids.append(app_jobs_with_stages)
    return apps_jobs_ids

def print_application_jobs_DAGs(jobs_graphs):
    for job_idx, job_graph in enumerate(jobs_graphs):
        print("******************************** Job {} stages DAG ********************************".format(job_idx))
        print("Nodes:")
        for node in list(job_graph.nodes(data=True)):
            print("stage_key: {}".format(node[0]))
            print("stage_id: {}".format(node[1]["stage_id"]))
            print("num_tasks: {}".format(node[1]["num_tasks"]))
            print("parent_ids: {}".format(node[1]["parent_ids"]))
        print("Edges:")
        print(list(job_graph.edges))

def get_application_jobs_DAGs(app_idx, app_id, apps_jobs_stages):
    jobs_graphs = []
    stages_keys = []
    print("Building jobs DAG for application {}..".format(app_id))
    for jobs_stages in apps_jobs_stages[app_idx]:
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerJobStart\" and .\"Job ID\"==" + str(jobs_stages[0]) + ")\'"
        #jobs_logs = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read().decode('utf-8')
        jobs_logs = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
        jobs_json = json.loads(jobs_logs)
        job_graph = nx.DiGraph()
        min_stage_id = sys.maxsize
        for stage in jobs_json["Stage Infos"]:
            if stage["Stage ID"] in jobs_stages[1]:
                min_stage_id = stage["Stage ID"] if stage["Stage ID"] < min_stage_id else min_stage_id
                # Add stage node to job stages DAG
                job_graph.add_node(stage["Stage ID"],
                        stage_key = stage["Stage Key"],
                        num_tasks = stage["Number of Tasks"],
                        #rdds_graph = rdds_graph,
                        parent_ids = stage["Parent IDs"])
                stages_keys.append(stage["Stage Key"])
        # Add edge to the current job stages DAG
        for node in list(job_graph.nodes(data=True)):
            for father_id in node[1]["parent_ids"]:
                if father_id >= min_stage_id:
                    job_graph.add_edge(father_id, node[0])
        jobs_graphs.append(job_graph)
    keys_unicity = len(set(stages_keys)) == len(stages_keys)
    if not keys_unicity:
        print("ERROR: Application {} does not have unique stage keys!".format(app_id))
        exit(1)
    if DEBUG:
        print_application_jobs_DAGs(jobs_graphs)
    return jobs_graphs

def check_number_of_edges_DAGs(ref_dag, dag):
    if len(list(ref_dag.edges)) != len(list(dag.edges)):
       # The reference and current DAG has a different number of stages
       return False
    return True

def check_number_of_stages_DAGs(ref_dag, dag):
    if len(list(ref_dag.nodes(data=True))) != len(list(dag.nodes(data=True))):
       # The reference and current DAG has a different number of stages
       return False
    return True

def check_RDDs_equivalence(first_rdd, second_rdd):
    if not (first_rdd["rdd_name"] == second_rdd["rdd_name"]
            and first_rdd["scope_name"] == second_rdd["scope_name"]
            and first_rdd["callsite"] == second_rdd["callsite"]
            and len(first_rdd["parent_ids"]) == len(second_rdd["parent_ids"])):
        return False
    return True

def get_sanity_check_on_RDDs_DAGs(ref_dag, dag, ref_stage_id, stage_id):
    num_stage_eq = check_number_of_stages_DAGs(ref_dag, dag)
    num_edges_eq = check_number_of_edges_DAGs(ref_dag, dag)
    ref_edges = list(ref_dag.edges) if len(list(ref_dag.edges)) == 1 else sorted(list(ref_dag.edges), key=lambda x: x[0])
    edges = list(dag.edges) if len(list(dag.edges)) == 1 else sorted(list(dag.edges), key=lambda x: x[0])
    edges_eq = True
    for ref_edge in ref_edges:
        edge_eq = False
        for edge in edges:
            src_RDDs_eq = check_RDDs_equivalence(ref_dag.nodes[ref_edge[0]], dag.nodes[edge[0]])
            dest_RDDs_eq = check_RDDs_equivalence(ref_dag.nodes[ref_edge[1]], dag.nodes[edge[1]])
            if src_RDDs_eq and dest_RDDs_eq:
                edge_eq = True
                break
        if not edge_eq:
            edges_eq = False
            break
    eq = num_stage_eq and num_edges_eq and edges_eq
    return eq
    
def get_sanity_check_on_stages_DAGs(ref_dag, dag):
    stages_DAGs_eq = True
    reord_stage_pairs = []
    ref_edges = list(ref_dag.edges) if len(list(ref_dag.edges)) == 1 else sorted(list(ref_dag.edges), key=lambda x: x[0])
    edges = list(dag.edges) if len(list(dag.edges)) == 1 else sorted(list(dag.edges), key=lambda x: x[0])
    for ref_edge in ref_edges:
        found_match = False
        for edge in edges:
            source_stage_eq = True if ref_dag.nodes[ref_edge[0]]['stage_key'] == dag.nodes[edge[0]]['stage_key'] \
                    and  ref_dag.nodes[ref_edge[0]]['num_tasks'] == dag.nodes[edge[0]]['num_tasks'] else False
            dest_stage_eq = True if ref_dag.nodes[ref_edge[1]]['stage_key'] == dag.nodes[edge[1]]['stage_key'] \
                    and ref_dag.nodes[ref_edge[1]]['num_tasks'] == dag.nodes[edge[1]]['num_tasks'] else False
            if source_stage_eq and dest_stage_eq:
                source_stage_pair = [ref_edge[0], edge[0]]
                if source_stage_pair not in reord_stage_pairs:
                    reord_stage_pairs.append(source_stage_pair)
                dest_stage_pair = [ref_edge[1], edge[1]]
                if dest_stage_pair not in reord_stage_pairs:
                    reord_stage_pairs.append(dest_stage_pair)
                found_match = True
                break
        if not found_match:
            stages_DAGs_eq = False
            break
    return stages_DAGs_eq, reord_stage_pairs

def compare_jobs_DAGs(ref_job_dag, job_dag):
    nstages_eq = check_number_of_stages_DAGs(ref_job_dag, job_dag)
    nedges_eq = check_number_of_edges_DAGs(ref_job_dag, job_dag)
    if nstages_eq and nedges_eq:
        if len(list(ref_job_dag.nodes(data=True))) == 1:
            ref_job_stage_key = list(ref_job_dag.nodes(data=True))[0][1]['stage_key']
            job_stage_key = list(job_dag.nodes(data=True))[0][1]['stage_key']
            ref_job_stage_ntasks  = list(ref_job_dag.nodes(data=True))[0][1]['num_tasks']
            job_stage_ntasks = list(job_dag.nodes(data=True))[0][1]['num_tasks']
            num_tasks_eq = True if ref_job_stage_ntasks == job_stage_ntasks else False
            stages_dags_eq = True if ref_job_stage_key == job_stage_key and num_tasks_eq else False
            reord_stage_pairs = [[list(ref_job_dag.nodes)[0], list(job_dag.nodes)[0]]]
        else:
            stages_dags_eq, reord_stage_pairs = get_sanity_check_on_stages_DAGs(ref_job_dag, job_dag)
        if stages_dags_eq:
            return True, reord_stage_pairs
    return False, []

def reorder_apps_dags(apps_dags, apps_ids, apps_jobs_stages, first_exp=False):
    reord_apps_dags = []
    ref_app_dags = apps_dags[0]
    reord_apps_jobs_stages = []
    reord_ref_app_jobs_stages = []
    global ref_info
    for idx_app, app_dags in enumerate(apps_dags[1:]):
        reord_app_dags = []
        reord_app_jobs_stages = []
        num_found_matches = 0
        for ref_idx_job_dag, ref_job_dag in enumerate(ref_app_dags):
            found_match = False
            # Search for equivalent job DAG in the remaining jobs in the list
            for idx_job_dag, job_dag in enumerate(app_dags):
                found_match, reord_stage_pairs = compare_jobs_DAGs(ref_job_dag, job_dag)
                if found_match:
                    reord_app_dags.append(job_dag)
                    del(app_dags[idx_job_dag])
                    if idx_app == 0 and not ref_info:
                        reord_ref_app_jobs_stages.append([apps_jobs_stages[0][ref_idx_job_dag][0], [x[0] for x in reord_stage_pairs]])
                    reord_app_jobs_stages.append([apps_jobs_stages[idx_app+1][idx_job_dag][0], [x[1] for x in reord_stage_pairs]])
                    del(apps_jobs_stages[idx_app+1][idx_job_dag])
                    num_found_matches += 1
                    break
            if not found_match:
                print("ERROR: Cannot find matching job for reference job {} in application {}!".format(apps_jobs_stages[0][ref_idx_job_dag][0], apps_ids[idx_app+1]))
        reord_apps_jobs_stages.append(reord_app_jobs_stages)
        reord_apps_dags.append(reord_app_dags)
    reord_apps_dags = [apps_dags[0]] + reord_apps_dags
    if not ref_info:
        reord_apps_jobs_stages = [reord_ref_app_jobs_stages] + reord_apps_jobs_stages
    else:
        reord_apps_jobs_stages =  [apps_jobs_stages[0]] + reord_apps_jobs_stages
    if first_exp:
        # Store the reference data
        ref_info = [[apps_ids[0]], reord_ref_app_jobs_stages, ref_app_dags]
    return reord_apps_dags, reord_apps_jobs_stages

def sync_across_exp(apps_dags, apps_ids, apps_jobs_stages):
    tmp_apps_ids = []
    tmp_apps_ids.append(ref_info[0])
    tmp_apps_ids.append(apps_ids[0])
    tmp_apps_jobs_stages = []
    tmp_apps_jobs_stages.append(ref_info[1])
    tmp_apps_jobs_stages.append(apps_jobs_stages[0])
    tmp_apps_dags = []
    tmp_apps_dags.append(ref_info[2])
    tmp_apps_dags.append(apps_dags[0])
    reord_apps_dags, reord_apps_jobs_stages = reorder_apps_dags(tmp_apps_dags, tmp_apps_ids, tmp_apps_jobs_stages)
    apps_dags[0] = reord_apps_dags[1]
    apps_jobs_stages[0] = reord_apps_jobs_stages[1]
    return apps_dags, apps_jobs_stages

def sanity_check_on_DAGs(apps_ids, apps_jobs_stages, s3_client, query, ds, n_exs=None, min_nexs=None, max_nexs=None):
    bucket = os.getenv('MINIO_LOGS_BUCKET').split('/')[0]
    if n_exs != None:
        prefix = 'logs/' + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-ex' + str(n_exs) + '-test'
    else:
        prefix = 'logs/' + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-dyn' + str(min_nexs) + "to" + str(max_nexs) + '-test'
    # Create for each application the DAG
    apps_dags = []
    for app_idx, app_id in enumerate(apps_ids):
        # Get the application event logs from MinIO
        s3_client.download_file(bucket, prefix + '/' + app_id, app_id)
        # Get application jobs DAGs
        apps_dags.append(get_application_jobs_DAGs(app_idx, app_id, apps_jobs_stages))
        os.remove(app_id)
    # Reorder jobs if necessary
    if (n_exs != None and EXECUTORS.index(n_exs) > 0) \
            or (n_exs == None):
        apps_dags, apps_jobs_stages = sync_across_exp(apps_dags, apps_ids, apps_jobs_stages)
        reord_apps_dags, reord_apps_jobs_stages = reorder_apps_dags(apps_dags, apps_ids, apps_jobs_stages)
    else:    
        reord_apps_dags, reord_apps_jobs_stages = reorder_apps_dags(apps_dags, apps_ids, apps_jobs_stages, True)
    return reord_apps_dags, reord_apps_jobs_stages
   
def restart_history_server(query, ds, num_exs=None, max_nexs=None):
    # Create temporary history server conf file
    cmd = "cp templ-spark-defaults.conf ../spark/conf/spark-defaults.conf"
    os.system(cmd)
    cmd = "sed -i 's/BENCHMARK_NAME/" + BENCHMARK_NAME + "/g' ../spark/conf/spark-defaults.conf"
    os.system(cmd)
    cmd = "sed -i 's/QUERY/" + query + "/g' ../spark/conf/spark-defaults.conf"
    os.system(cmd)
    if num_exs != None and max_nexs == None:
        cmd = "sed -i 's/VALEXS/ex" + str(num_exs) + "/g' ../spark/conf/spark-defaults.conf"
    elif num_exs != None and max_nexs != None:
        cmd = "sed -i 's/VALEXS/dyn" + str(num_exs) + "to" + str(max_nexs) + "/g' ../spark/conf/spark-defaults.conf"
    os.system(cmd)
    cmd = "sed -i 's/DS/" + str(ds) + "/g' ../spark/conf/spark-defaults.conf"
    os.system(cmd)
    # Stop history server if running
    cmd = "../spark/sbin/stop-history-server.sh"
    out = os.popen(cmd).read()
    # Start history server
    cmd = "../spark/sbin/start-history-server.sh"
    proc = subprocess.Popen([cmd], shell=False, stdout=subprocess.PIPE)
    proc.wait()
    time.sleep(2)

def setup_minio_client():
    s3_session = boto3.session.Session()
    s3_client = s3_session.client(
        service_name='s3',
        endpoint_url = os.getenv('S3A_ENDPOINT'),
        aws_access_key_id = os.getenv('S3A_ACCESS_KEY'),
        aws_secret_access_key= os.getenv('S3A_SECRET_KEY')
    )
    return s3_client

def check_executors_ready(apps_ids, s3_client, query, ds, n_exs):
    bucket = os.getenv('MINIO_LOGS_BUCKET').split('/')[0]
    prefix = 'logs/' + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-ex' + str(n_exs) + '-test'
    for app_idx, app_id in enumerate(apps_ids):
        # Get the application event logs from MinIO
        s3_client.download_file(bucket, prefix + '/' + app_id, app_id)
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerExecutorAdded\") | .\"Executor ID\"\'"
        exs_logs = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
        os.remove(app_id)
        used_exs = len(exs_logs.split('\n')) - 1
        if used_exs != n_exs:
            print("ERROR: The run {} has used only {}/{} executors!".format(app_id, used_exs, n_exs))
            exit(1)
    print("All applications have used {} executors!".format(n_exs))

def dump_statistics(query, ds, jobs_stages, exs_stats, exs_times):
    # Get application runtimes
    if not os.path.exists('files/' + BENCHMARK_NAME):
        os.makedirs('files/' + BENCHMARK_NAME) 
    with open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_application.csv", "w") as out_file:
        for idx_exs, num_exs  in enumerate(EXECUTORS):
            # [num_exs, avt_runtime, median_runtime, min_runtime, max_runtime]
            line = str(num_exs) + ','
            line += str(exs_stats[idx_exs][0][0]) + ','
            line += str(exs_stats[idx_exs][0][1]) + ','
            line += str(exs_stats[idx_exs][0][2]) + ','        
            line += str(exs_stats[idx_exs][0][3]) + '\n'
            out_file.write(line)
    # Get stages runtimes
    with open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_stages.csv", "w") as out_file:
        for idx_exs, num_exs in enumerate(EXECUTORS):
            for idx_job, job_stages in enumerate(jobs_stages):
                for idx_stage, stage_id in enumerate(job_stages[1]):
                    # [num_exs, jobid, stageid, avg_runtime, acc_tasks_runtime, median_runtime, min_runtime, max_runtime]
                    avg_runtime = exs_stats[idx_exs][1][idx_job][1][idx_stage][0]
                    acc_tasks_runtime = exs_stats[idx_exs][1][idx_job][1][idx_stage][1]
                    median_runtime =  exs_stats[idx_exs][1][idx_job][1][idx_stage][2]
                    min_runtime = exs_stats[idx_exs][1][idx_job][1][idx_stage][3]
                    max_runtime = exs_stats[idx_exs][1][idx_job][1][idx_stage][4]
                    line = str(num_exs) + ','
                    line += str(job_stages[0]) + ',' 
                    line += str(stage_id) + ','
                    line += str(avg_runtime) + ','
                    line += str(acc_tasks_runtime) + ','
                    line += str(median_runtime) + ','
                    line += str(min_runtime) + ','
                    line += str(max_runtime) + '\n'
                    out_file.write(line)
    with open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_exs_times.csv", "w") as out_file:
        # output: [min_exs, max_exs, avg_runtime, median_runtime, min_runtime, max_runtime]
        for idx_exs, num_exs in enumerate(EXECUTORS):
            line = str(num_exs) + ','
            line += str(exs_times[idx_exs][0]) + ','
            line += str(exs_times[idx_exs][1]) + ','
            line += str(exs_times[idx_exs][2]) + ','        
            line += str(exs_times[idx_exs][3]) + '\n'
            out_file.write(line)

def dump_statistics_dyn(query, ds, jobs_stages, dyn_stats, pairs_exs, exs_times):
    # Get application runtimes
    if not os.path.exists('files/' + BENCHMARK_NAME):
        os.makedirs('files/' + BENCHMARK_NAME) 
    with open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_dyn_application.csv", "w") as out_file:
        # [min_exs, max_exs, avg_runtime, median_runtime, min_runtime, max_runtime]
        for idx_pair, pair in enumerate(pairs_exs):
            line = str(pair[0]) + ','
            line += str(pair[1]) + ','
            line += str(dyn_stats[idx_pair][0][0]) + ','
            line += str(dyn_stats[idx_pair][0][1]) + ','
            line += str(dyn_stats[idx_pair][0][2]) + ','        
            line += str(dyn_stats[idx_pair][0][3]) + '\n'
            out_file.write(line)
    # Get stages runtimes
    with open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_dyn_stages.csv", "w") as out_file:
        for idx_pair, pair in enumerate(pairs_exs):
            for idx_job, job_stages in enumerate(jobs_stages):
                for idx_stage, stage_id in enumerate(job_stages[1]):
                    # output: [min_exs, max_exs, jobid, stageid, avg_runtime, acc_tasks_runtime, median_runtime, min_runtime, max_runtime]
                    avg_runtime = dyn_stats[idx_pair][1][idx_job][1][idx_stage][0]
                    acc_tasks_runtime = dyn_stats[idx_pair][1][idx_job][1][idx_stage][1]
                    median_runtime =  dyn_stats[idx_pair][1][idx_job][1][idx_stage][2]
                    min_runtime = dyn_stats[idx_pair][1][idx_job][1][idx_stage][3]
                    max_runtime = dyn_stats[idx_pair][1][idx_job][1][idx_stage][4]
                    line = str(pair[0]) + ','
                    line += str(pair[1]) + ','
                    line += str(job_stages[0]) + ',' 
                    line += str(stage_id) + ','
                    line += str(avg_runtime) + ','
                    line += str(acc_tasks_runtime) + ','
                    line += str(median_runtime) + ','
                    line += str(min_runtime) + ','
                    line += str(max_runtime) + '\n'
                    out_file.write(line)
    with open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_dyn_exs_times.csv", "w") as out_file:
        # output: [min_exs, max_exs, avg_runtime, median_runtime, min_runtime, max_runtime]
        for idx_pair, pair in enumerate(pairs_exs):
            line = str(pair[0]) + ','
            line += str(pair[1]) + ','
            line += str(exs_times[idx_pair][0]) + ','
            line += str(exs_times[idx_pair][1]) + ','
            line += str(exs_times[idx_pair][2]) + ','        
            line += str(exs_times[idx_pair][3]) + '\n'
            out_file.write(line)

def main():
    print("********** APPLICATION, JOBS, STAGES, and TASKS STATISTICS **********")
    s3_client = setup_minio_client()
    ref_jobs_stages = []
    for ds in DATA_SIZE:
        for query in QUERIES:
            exs_stats = []
            exs_times = []
            for idx_ex, n_exs in enumerate(EXECUTORS):
                print("Analyzing {} {} query on data size {} with {} executors..".format(BENCHMARK_NAME, query, ds, n_exs))
                # Restart history server to read from correct logs bucket
                restart_history_server(query, ds, n_exs)
                # Get applicationsIds
                apps_ids = get_list_applications_ids()
                # Check all assigned executors are ready for the application
                #check_executors_ready(apps_ids, s3_client, query, ds, n_exs)
                # Get list of all applications jobs stages with relative ids
                apps_jobs_stages = get_apps_jobs_with_stages(apps_ids)
                # Sanity check over DAGs
                reord_apps_dags, reord_apps_jobs_stages = sanity_check_on_DAGs(apps_ids, apps_jobs_stages, s3_client, query, ds, n_exs = n_exs)
                # Store jobs and stages ids 
                if EXECUTORS.index(n_exs) == 0:
                    ref_jobs_stages = reord_apps_jobs_stages[0]
                # Get applications, jobs, and stages average runtimes and task accumulated runtimes 
                exs_stats.append(get_statistics(apps_ids, reord_apps_jobs_stages))
                exs_times.append(get_avg_exs_time(apps_ids, s3_client, query, ds, n_exs, n_exs))
            # Dump statistics on a file
            dump_statistics(query, ds, ref_jobs_stages, exs_stats, exs_times)
            # Get statistics for dynamic setup if required
            if dynamic == True:
                dyn_stats = []
                exs_times = []
                pairs_exs = [[EXECUTORS[0], exs] for exs in EXECUTORS[1:]]
                for pair in pairs_exs:
                    print("Analyzing {} {} query on data size {} with dynamic allocation of executors [{}, {}]..".format(BENCHMARK_NAME, query, ds, pair[0], pair[1]))
                    # Restart history server to read from correct logs bucket
                    restart_history_server(query, ds, pair[0], pair[1])
                    # Get applicationsIds
                    apps_ids = get_list_applications_ids()
                    # Get list of all applications jobs stages with relative ids
                    apps_jobs_stages = get_apps_jobs_with_stages(apps_ids)
                    # Sanity check over DAGs
                    reord_apps_dags, reord_apps_jobs_stages = sanity_check_on_DAGs(apps_ids, apps_jobs_stages, s3_client, query, ds, min_nexs = pair[0], max_nexs = pair[1])
                    # Get application, jobs, and stages average runtimes and task accumulated runtimes 
                    dyn_stats.append(get_statistics(apps_ids, reord_apps_jobs_stages))
                    exs_times.append(get_avg_exs_time(apps_ids, s3_client, query, ds, pair[0], pair[1]))
                # Dump statistics on a file
                dump_statistics_dyn(query, ds, ref_jobs_stages, dyn_stats, pairs_exs, exs_times)

if __name__ == "__main__":
    main()

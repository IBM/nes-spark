import subprocess
import json
import os
import time 
import boto3
import re
import sys
import random
import networkx as nx
import matplotlib.pyplot as plt
plt.rcParams.update({'figure.max_open_warning': 0})
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
from operator import itemgetter
from datetime import datetime
import matplotlib.patheffects as mpe

#NOTE: Set correct logs folder path in spark configuration file spark-defaults.conf before running

DEBUG = False

# Define benchmark name (either tpch-sql or tpcds-sql
BENCHMARK_NAME = 'tpcds-sql'

# Define number of iterations
NUM_ITERATIONS = 1

# Define list of number of executors
EXECUTORS = [ 1,
    10,
    20,
    30,
    40,
    50
]

EXECUTORS = [ 20 ]

#Define number of cores (tasks) per executor
TASKS_PER_EXECUTOR = 2 

# Define list of input data sizes
DATA_SIZE = 100

# Define query to extract tasks runtimes of
QUERY = "q72"

#history_server = 'http://localhost:18080/api/v1/'
history_server = ''

# Define global variable for the reference job stages to use when changing the number of executors
ref_info = []

TEST = True

def get_info_from_history_server(request):
    result = subprocess.run(['curl', '-s', history_server + request], capture_output=True, text=True)
    content = result.stdout
    json_data = json.loads(content)
    return json_data

def get_list_applications_ids():
    request = 'applications/'
    json_data = get_info_from_history_server(request)
    list_apps = []
    for i in range(NUM_ITERATIONS):
        list_apps.append(json_data[i]['id'])
    return list_apps

def get_list_jobs_with_stages(app_id):
    app_request = 'applications/' + app_id 
    jobs_request = app_request + '/jobs/'
    jobs_json_data = get_info_from_history_server(jobs_request)
    list_jobs = []
    for idx_job in range(len(jobs_json_data)):
        temp_stages_ids = jobs_json_data[idx_job]['stageIds']
        stages_ids = []
        for stage_id in temp_stages_ids:
            stage_request = app_request + '/stages/' + str(stage_id)
            stage_json_data = get_info_from_history_server(stage_request)[0]
            if stage_json_data['status'] == 'COMPLETE':
                stages_ids.append(stage_id)
        list_jobs.append([jobs_json_data[idx_job]['jobId'], stages_ids])
    return list_jobs

def get_diff_timestamps(json_data):
    first_task_ts = datetime.strptime(json_data['firstTaskLaunchedTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')
    compl_ts = datetime.strptime(json_data['completionTime'], '%Y-%m-%dT%H:%M:%S.%fGMT')
    return (compl_ts - first_task_ts).total_seconds(), first_task_ts
   

def get_apps_jobs_with_stages(apps_ids):
    # Get list of jobs with relative stages 
    apps_jobs_ids = []
    for app_id in apps_ids:
        app_jobs_with_stages = get_list_jobs_with_stages(app_id)
        app_jobs_with_stages.reverse()
        apps_jobs_ids.append(app_jobs_with_stages)
    return apps_jobs_ids

def contains_parentesis(string):
    present = False
    if len(re.findall(r'\[\]\{\}',string)) > 0:
        present = True
    return present

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
    print("Building jobs DAG for application {}..".format(app_id))
    for jobs_stages in apps_jobs_stages[app_idx]:
        cmd = "cat " + app_id + " | jq \'. | select(.Event==\"SparkListenerJobStart\" and .\"Job ID\"==" + str(jobs_stages[0]) + ")\'"
        jobs_logs = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
        jobs_json = json.loads(jobs_logs)
        job_graph = nx.DiGraph()
        min_stage_id = sys.maxsize
        for stage in jobs_json["Stage Infos"]:
            if stage["Stage ID"] in jobs_stages[1]:
                min_stage_id = stage["Stage ID"] if stage["Stage ID"] < min_stage_id else min_stage_id
                job_graph.add_node(stage["Stage ID"],
                        stage_key = stage["Stage Key"],
                        num_tasks = stage["Number of Tasks"],
                        parent_ids = stage["Parent IDs"])
        # Add edge to the current job stages DAG
        for node in list(job_graph.nodes(data=True)):
            for father_id in node[1]["parent_ids"]:
                if father_id >= min_stage_id:
                    job_graph.add_edge(father_id, node[0])
        jobs_graphs.append(job_graph)
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

def get_sanity_check_on_stages_DAGs(ref_dag, dag):
    stages_DAGs_eq = True
    reord_stage_pairs = []
    ref_edges = list(ref_dag.edges) if len(list(ref_dag.edges)) == 1 else sorted(list(ref_dag.edges), key=lambda x: x[0])
    edges = list(dag.edges) if len(list(dag.edges)) == 1 else sorted(list(dag.edges), key=lambda x: x[0])
    for ref_edge in ref_edges:
        found_match = False
        for edge in edges:
            source_stage_eq = True if ref_dag.nodes[ref_edge[0]]['stage_key'] == dag.nodes[edge[0]]['stage_key'] \
                    and ref_dag.nodes[ref_edge[0]]['num_tasks'] == dag.nodes[edge[0]]['num_tasks'] else False
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

def sanity_check_on_DAGs(apps_ids, apps_jobs_stages, s3_client, query, ds, n_exs):
    bucket = os.getenv('MINIO_LOGS_BUCKET').split('/')[0]
    if TEST:
        prefix = 'logs/test-custom-spark-' + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-ex' + str(n_exs)
    else:
        prefix = 'logs/' + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-ex' + str(n_exs) + '-test'
    # Create for each application the DAG
    apps_dags = []
    for app_idx, app_id in enumerate(apps_ids):
        # Get the application event logs from MinIO
        s3_client.download_file(bucket, prefix + '/' + app_id, app_id)
        # Get application jobs DAGs
        apps_dags.append(get_application_jobs_DAGs(app_idx, app_id, apps_jobs_stages))
        os.remove(app_id)
    #if NUM_ITERATIONS == 1 and len(EXECUTORS) == 1:
    if NUM_ITERATIONS == 1 and EXECUTORS.index(n_exs)  == 0:
        # No need to reorder jobs since there is only one application
        return apps_dags, apps_jobs_stages
    # Reorder dags and jobs stages
    if EXECUTORS.index(n_exs) > 0:
        apps_dags, apps_jobs_stages = sync_across_exp(apps_dags, apps_ids, apps_jobs_stages)
        reord_apps_dags, reord_apps_jobs_stages = reorder_apps_dags(apps_dags, apps_ids, apps_jobs_stages)
    else:    
        reord_apps_dags, reord_apps_jobs_stages = reorder_apps_dags(apps_dags, apps_ids, apps_jobs_stages, True)
    return reord_apps_dags, reord_apps_jobs_stages
   
def restart_history_server(query, ds, num_exs):
    # Create temporary history server conf file
    if TEST:
        cmd = "cp templ-test-custom-spark-defaults.conf ../spark/conf/spark-defaults.conf" 
        os.system(cmd)
    else:
        cmd = "cp templ-spark-defaults.conf ../spark/conf/spark-defaults.conf"
        os.system(cmd)
    cmd = "sed -i 's/BENCHMARK_NAME/" + BENCHMARK_NAME + "/g' ../spark/conf/spark-defaults.conf"
    os.system(cmd)
    cmd = "sed -i 's/QUERY/" + query + "/g' ../spark/conf/spark-defaults.conf"
    os.system(cmd)
    cmd = "sed -i 's/VALEXS/ex" + str(num_exs) + "/g' ../spark/conf/spark-defaults.conf"
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
    print("Restarted history server to read {} events logs with num_exs {}".format(query, num_exs))

def setup_minio_client():
    s3_session = boto3.session.Session()
    s3_client = s3_session.client(
        service_name='s3',
        endpoint_url = os.getenv('S3A_ENDPOINT'),
        aws_access_key_id = os.getenv('S3A_ACCESS_KEY'),
        aws_secret_access_key= os.getenv('S3A_SECRET_KEY')
    )
    return s3_client

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

def parse_logs(n_exs, app_id, s3_client):
    bucket = os.getenv('MINIO_DLOGS_BUCKET').split('/')[0]
    if TEST:
        prefix = 'driver_logs/test-custom-spark-' + BENCHMARK_NAME + '-' + QUERY + '-sf100-ex' + str(n_exs)
    else:
        prefix = 'driver_logs/' + BENCHMARK_NAME + '-' + QUERY + '-sf100-ex' + str(n_exs) + '-test'
    s3_client.download_file(bucket, prefix + '/' + app_id + ".log", app_id + ".log")
    cmd = "grep -i \"RUNNING\" < " + app_id + ".log | grep -i \"Custom\""
    logs = (subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')).split("\n")[:-1]
    ref_ts = int(logs[0].split(" ")[6])
    # tasks_info format data: [taskId: [stageId, executorId, ts_start, ts_end]]
    tasks_info = {}
    for line in logs:
        line = line.split(" ")
        tasks_info.update({int(line[8]): [int(line[11]), int(line[13]), int(line[6])]})
    cmd = "grep -i \"FINISHED\" < " + app_id + ".log | grep -i \"Custom\""
    logs = (subprocess.run(cmd, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')).split("\n")[:-1]
    for line in logs:
        line = line.split(" ")
        vals = tasks_info[int(line[8])]
        tasks_info.update({int(line[8]): [vals[0], vals[1], vals[2] - ref_ts, int(line[6]) - ref_ts]})
    return tasks_info
    
def select_n_random_colors(n):
    rand = lambda: random.randint(60, 255)
    colors = ['#%02X%02X%02X' % (rand(), rand(), rand()) for i in range(n)]
    return colors

def get_list_app_stages(jobs_stages):
    stages = [] 
    for job in jobs_stages:
        for stage in job[1]:
            stages.append(stage)
    return stages

def get_stage_idx(task_stage, app_jobs_stages):
    app_stages = get_list_app_stages(app_jobs_stages)
    idx_stage = app_stages.index(task_stage)
    return idx_stage

def search_ref_color(colors, task_stage, app_jobs_stages):
    stage_idx  = get_stage_idx(task_stage,  app_jobs_stages)
    return colors[stage_idx]

def plot_tasks(idx_app, tasks_info, colors, ax, stages_plots, n_exs, ref_jobs_stages, reord_app_jobs_stages, apps_xmax):
    exs_last_task_end = [[-1, -1] for x in range(n_exs)]
    outline = mpe.withStroke(linewidth=14.6, foreground='black')
    current_stage = -1
    stage_color = search_ref_color(colors, list(tasks_info.values())[0][0], reord_app_jobs_stages)
    num_stages = max([x[0] for x in tasks_info.values()]) + 1
    num_compl_stages = len(colors)
    num_skipped_stages = num_stages - num_compl_stages
    executors_tasks = [[[0, []] for y in range(len(colors))] for x in range(n_exs)]
    for taskId, task_info in zip(tasks_info.keys(), tasks_info.values()):
        found_slot = False
        if task_info[0] != current_stage:
            stage_color = search_ref_color(colors, task_info[0], reord_app_jobs_stages)
            current_stage = task_info[0]
        for c in range(TASKS_PER_EXECUTOR):
            if exs_last_task_end[task_info[1] - 1][c] <= task_info[2]:
                exs_last_task_end[task_info[1] - 1][c] = 0
        core = exs_last_task_end[task_info[1] - 1].index(min(exs_last_task_end[task_info[1] - 1])) 
        if exs_last_task_end[task_info[1] - 1][core] <= task_info[2]:
            y_val =  TASKS_PER_EXECUTOR*(task_info[1] - 1) + core
            xmin_val = task_info[2]/1000 
            xmax_val = task_info[3]/1000
            ax.hlines(y = y_val, xmin = xmin_val, xmax = xmax_val, color = stage_color, lw = 14, path_effects=[outline])
            stage_plot_idx = get_stage_idx(task_info[0], reord_app_jobs_stages)
            stages_plots[stage_plot_idx][1].hlines(y = y_val, xmin = xmin_val,  xmax = xmax_val, color = stage_color, lw = 14, path_effects=[outline])
            exs_last_task_end[task_info[1] - 1][core] = task_info[3]
            idx_stage = (task_info[0] + 1) - num_skipped_stages - 1
            executors_tasks[task_info[1]-1][idx_stage][0] = task_info[1]
            executors_tasks[task_info[1]-1][idx_stage][1].append([task_info[2]/1000, task_info[3]/1000])
            found_slot = True
        if not found_slot:
            print("ERROR: Cannot find a free spot for task {} in stage {} in executor {}!".format(taskId, task_info[0], task_info[1]))
            print("Task Info:")
            print("{}: [{}, {}, {}, {}]".format(taskId, task_info[0], task_info[1], task_info[2], task_info[3]))
            print("Executors Info:")
            print(exs_last_task_end)
            print("Skipping current application plots..")
            break 
    if found_slot:
        total_cost = 0
        for ex_tasks in executors_tasks:
            for stage_tasks in ex_tasks:
                if len(stage_tasks[1]) > 0:
                    sorted_start = sorted(stage_tasks[1], key=lambda x: x[0])
                    sorted_end = sorted(stage_tasks[1], key=lambda x: x[1])
                    total_cost += sorted_end[-1][1] - sorted_start[0][0]
    print("Total cost is: {} units".format(total_cost))
    return found_slot, ax, stages_plots

def get_tasks_info(n_exs, apps_ids, s3_client):
    apps_tasks_info = []
    for idx_app, app_id in enumerate(apps_ids):
        print("Retrieving tasks info for application {}..".format(app_id))
        apps_tasks_info.append(parse_logs(n_exs, app_id, s3_client))
        os.remove(app_id + ".log")
    return apps_tasks_info

def get_max_xvalue(exs_tasks_info, idx_start, idx_end):
    apps_xmaxs = []
    if idx_start == idx_end:
        apps_xmaxs.append(list(exs_tasks_info[0][0].values())[-1][3])
    else:
        for idx_ex in range(idx_start, idx_end):
            for it in range(NUM_ITERATIONS):
                apps_xmaxs.append(list(exs_tasks_info[idx_ex][it].values())[-1][3])
    return max(apps_xmaxs)

def get_tasks_plots(exs_apps_ids, ref_jobs_stages, exs_reord_apps_jobs_stages, exs_tasks_info):
    num_stages = len(get_list_app_stages(ref_jobs_stages))
    colors = select_n_random_colors(num_stages)
    apps_xmax = get_max_xvalue(exs_tasks_info, 1, len(EXECUTORS))
    ref_stages = get_list_app_stages(ref_jobs_stages)
    # Create legend with correct stage number and overall stage number of tasks
    list_patches = []
    for s_idx, stage_id in enumerate(ref_stages):
        stage_ntasks = [t[0] for t in exs_tasks_info[0][0].values()].count(stage_id)
        list_patches.append(mpatches.Patch(color=colors[s_idx], label='Stage' + str(stage_id) + ' (NT=' + str(stage_ntasks)  + ')'))
    # Create application and stages plots
    for idx_ex, n_exs in enumerate(EXECUTORS):
        for idx_app, app_id in enumerate(exs_apps_ids[idx_ex]):
            print("Building plots for application {}..".format(app_id))
            fig, ax = plt.subplots()
            stages_plots = []
            for x in range(num_stages):
                fig_tmp, ax_tmp = plt.subplots()
                stages_plots.append([fig_tmp, ax_tmp])
            found_all_slots, ax, stages_plots = plot_tasks(idx_app, exs_tasks_info[idx_ex][idx_app], colors, ax, stages_plots, n_exs, ref_jobs_stages, exs_reord_apps_jobs_stages[idx_ex][idx_app], apps_xmax)
            ax.set_xlabel('Time [s]')
            x_max_limit = get_max_xvalue(exs_tasks_info, 0, 1)/1000 if n_exs == 1 else apps_xmax/1000 
            ax.set_xlim(left = 0, right = x_max_limit)
            ax.set_yticks([x for x in range(n_exs*TASKS_PER_EXECUTOR)], ["E" + str(e) + "C" + str(c) for e in range(1, n_exs+1) for c in range(1, TASKS_PER_EXECUTOR+1)])
            ax.set_ylabel('Executors')
            legend = ax.legend(handles = list_patches, ncol=2, loc='center left', bbox_to_anchor=(1, 0.5))
            _, fig_height = fig.get_size_inches()
            fig.canvas.draw()
            fig_x_inches = get_max_xvalue(exs_tasks_info, 0, 1)/1000/6 + legend.get_window_extent().width/100 if n_exs == 1 else apps_xmax/1000/6
            fig_y_inches = fig_height if n_exs == 1 else n_exs*0.7
            fig.set_size_inches(fig_x_inches, fig_y_inches)
            fig.tight_layout()
            if TEST:
                app_fpath = 'plots/' + BENCHMARK_NAME + '/' + QUERY + '_sf100/applications/' + str(n_exs) + 'exs/test_custom/' + app_id
            else:
                app_fpath = 'plots/' + BENCHMARK_NAME + '/' + QUERY + '_sf100/applications/' + str(n_exs) + 'exs/' + app_id
            if not os.path.exists(app_fpath):
                os.makedirs(app_fpath)
            if found_all_slots:
                fig.savefig(app_fpath + '/application.pdf')
            plt.close(fig) 
            for plot_idx, plot in enumerate(stages_plots):
                plot[1].set_xlabel('Time [s]')
                plot[1].set_yticks(range(n_exs*TASKS_PER_EXECUTOR), \
                        ["E" + str(e) + "C" + str(c) for e in range(1, n_exs+1) for c in range(1, TASKS_PER_EXECUTOR+1)])
                plot[1].use_sticky_edges = False
                plot[1].set_ylim(-1, n_exs*TASKS_PER_EXECUTOR)
                plot[1].set_ylabel('Executors')
                fig_width, fig_height = plot[0].get_size_inches()
                fig_height = fig_height if n_exs == 1 else n_exs*0.7
                plot[0].set_size_inches(fig_width, fig_height)
                if found_all_slots:
                    plot[0].savefig(app_fpath + '/stage' + str(ref_stages[plot_idx])  + '.pdf')
                plt.close(plot[0])

def main():
    print("********** TASKS RUNTIME ANALYSIS **********")
    exs_tasks_info = []
    exs_apps_ids = []
    exs_reord_apps_jobs_stages = []
    s3_client = setup_minio_client()
    ref_jobs_stages = []
    for idx_ex, n_exs in enumerate(EXECUTORS):
        print("Analyzing {} {} query on data size {} with {} executors..".format(BENCHMARK_NAME, QUERY, DATA_SIZE, n_exs))
        # Restart history server to read from correct logs bucket
        restart_history_server(QUERY, DATA_SIZE, n_exs)
        # Get applicationsIds
        apps_ids = get_list_applications_ids()
        exs_apps_ids.append(apps_ids)
        # Get list of all applications jobs stages with relative ids
        apps_jobs_stages = get_apps_jobs_with_stages(apps_ids)
        # Sanity check over DAGs
        reord_apps_dags, reord_apps_jobs_stages = sanity_check_on_DAGs(apps_ids, apps_jobs_stages, s3_client, QUERY, DATA_SIZE, n_exs)
        exs_reord_apps_jobs_stages.append(reord_apps_jobs_stages)
        # Store jobs and stages ids 
        if EXECUTORS.index(n_exs) == 0:
            ref_jobs_stages = reord_apps_jobs_stages[0]
        # Get tasks runtimes plots
        exs_tasks_info.append(get_tasks_info(n_exs, apps_ids, s3_client))
    # Get and store plots
    get_tasks_plots(exs_apps_ids, ref_jobs_stages, exs_reord_apps_jobs_stages, exs_tasks_info)

if __name__ == "__main__":
    main()

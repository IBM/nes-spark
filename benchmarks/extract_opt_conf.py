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
import csv 
import statistics

# Define list of number of executors
EXECUTORS = [ 1,
    10,
    20,
    30,
    40,
    50
#    60,
#    70,
#    80,
#    90,
#    100
]

# Define list of input data sizes
DATA_SIZE = [ 100
]

# Either tpch or tpcds-sql
BENCHMARK_NAME = "tpcds-sql"

QUERIES = [
#    "q9"
#    "q14a",
#    "q67"
    "q72"
#    "q95"
#    "q23a"
]

DEBUG = True

def get_query_avg_runtime(query, ds):
    file = open('files/' + BENCHMARK_NAME + '/' + query + '_sf' + str(ds) + '_application.csv')
    csvreader = csv.reader(file)
    data = []
    for row in csvreader:
        # Row information: [num_exs, app_avgruntime, app_medruntime, app_min_runtime, app_max_runtime]
        data.append(row[1])        
    file.close()
    return data 

def get_stages_avg_runtimes(query, ds):
    file = open('files/' + BENCHMARK_NAME + '/' + query + '_sf' + str(ds) + '_stages.csv')
    csvreader = csv.reader(file)
    tmp_num_exs = EXECUTORS[0]
    exs_data = []
    data = []
    for row in csvreader:
        # Row information: [num_exs, job_id, stage_id, stage_avgruntime, stage_tasks_acctime, stage_medruntime, stage_min_runtime, stage_max_runtime]
        if int(row[0]) == tmp_num_exs:
            exs_data.append([int(row[2]), row[3]])
        else:
            exs_data = sorted(exs_data, key=lambda x: x[0])
            exs_data = [float(stage[1]) for stage in exs_data]
            data.append(exs_data)
            tmp_num_exs = int(row[0])
            exs_data = []
            exs_data.append([int(row[2]), row[3]])
    exs_data = sorted(exs_data, key=lambda x: x[0])
    exs_data = [float(stage[1]) for stage in exs_data] 
    data.append(exs_data)
    file.close()
    return data 

def get_app_opt_nexs(query_avg_runtimes):
    ref_runtime = float(query_avg_runtimes[0])
    ref_cost = float(query_avg_runtimes[0])
    idx_exs = 1
    opt_nexs = 1
    while(idx_exs < len(EXECUTORS)):
        curr_runtime = float(query_avg_runtimes[idx_exs])
        curr_cost = float(query_avg_runtimes[idx_exs]) * EXECUTORS[idx_exs]
        perf_inc = ref_runtime/curr_runtime
        cost_inc = curr_cost/ref_cost
        if perf_inc > cost_inc:
            # Continue the search 
            opt_nexs = EXECUTORS[idx_exs]
            ref_runtime = curr_runtime
            ref_cost = curr_cost
        elif perf_inc == cost_inc:
            # Current nexs is the optimal one
            opt_nexs = EXECUTORS[idx_exs]
            ref_runtime = curr_runtime
            ref_cost = curr_cost
            break
        else:
            # Previous nexs is the optimal one
            opt_nexs = EXECUTORS[idx_exs-1]
            break
        print("Current opt_nexs {}".format(opt_nexs))
        idx_exs += 1
    return ref_runtime, ref_cost, opt_nexs

def get_conf_opt_nexs(stages_data, ds, query):
    n_stages = len(stages_data[0])
    opt_conf = [ 1 for x in range(n_stages)]
    curr_runtime_stages = stages_data[0].copy()
    ref_runtime = sum(stages_data[0])
    ref_cost = sum(stages_data[0])
    it = 0
    with open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_opt_confs.csv", "w") as out_file:
        while True:
            if DEBUG:
                print("*************** ITERATION {} ***************".format(it))
            it += 1
            diffs = []
            for idx_stage in range(n_stages):
                idx_curr_nexs = EXECUTORS.index(opt_conf[idx_stage])
                diffs.append(stages_data[idx_curr_nexs+1][idx_stage]-stages_data[idx_curr_nexs][idx_stage])
            max_perf_inc = min(diffs)
            stage_to_opt = diffs.index(max_perf_inc)
            idx_curr_nexs = EXECUTORS.index(opt_conf[stage_to_opt])
            tmp_opt_conf = opt_conf.copy()
            tmp_opt_conf[stage_to_opt] = EXECUTORS[idx_curr_nexs+1]
            tmp_curr_runtime_stages = curr_runtime_stages.copy()
            tmp_curr_runtime_stages[stage_to_opt] = stages_data[idx_curr_nexs+1][stage_to_opt]
            curr_runtime = sum(tmp_curr_runtime_stages)
            curr_cost = sum([runtime * nexs for runtime, nexs in zip(tmp_curr_runtime_stages, tmp_opt_conf)])
            perf_inc = ref_runtime/curr_runtime
            cost_inc = curr_cost/ref_cost
            if perf_inc > cost_inc:
                # Continue the search 
                opt_conf = tmp_opt_conf.copy()
                curr_runtime_stages = tmp_curr_runtime_stages.copy()
                if DEBUG:
                    print("Optimizing Stage: {} (from {} s to {} s)".format(stage_to_opt, stages_data[idx_curr_nexs][stage_to_opt], stages_data[idx_curr_nexs+1][stage_to_opt]))
                    print("Current Runtime: {} s".format(curr_runtime))
                    print("Previous Runtime: {} s".format(ref_runtime))
                    print("Current Cost: {} units".format(curr_cost))
                    print("Previous Cost: {} units".format(ref_cost))
                    print("Perf Increase: {}".format(perf_inc))
                    print("Cost Increase: {}".format(cost_inc))
                ref_runtime = curr_runtime
                ref_cost = curr_cost
                line = str(ref_runtime) + ','
                line += str(ref_cost) + '\n'
                out_file.write(line)
            elif perf_inc == cost_inc:
                # Current nexs is the optimal one
                opt_conf = tmp_opt_conf.copy()
                if DEBUG:
                    print("Optimizing Stage: {}(from {} s to {} s)".format(stage_to_opt, stages_data[idx_curr_nexs][stage_to_opt], stages_data[idx_curr_nexs+1][stage_to_opt]))
                    print("Current Runtime: {} s".format(curr_runtime))
                    print("Previous Runtime: {} s".format(ref_runtime))
                    print("Current Cost: {} units".format(curr_cost))
                    print("Previous Cost: {} units".format(ref_cost))
                    print("Perf Increase: {}".format(perf_inc))
                    print("Cost Increase: {}".format(cost_inc))
                ref_runtime = curr_runtime
                ref_cost = curr_cost
                line = str(ref_runtime) + ','
                line += str(ref_cost)
                out_file.write(line)
                break
            else:
                # Previous nexs is the optimal one
                break
    return ref_runtime, ref_cost, opt_conf

def get_runtimes_and_costs_fix_nexs(query_avg_runtimes):
    for idx_exs in range(len(EXECUTORS)):
        curr_runtime = float(query_avg_runtimes[idx_exs])
        curr_cost = float(query_avg_runtimes[idx_exs]) * EXECUTORS[idx_exs]
        print("With a fixed number of {} executor(s):".format(EXECUTORS[idx_exs]))
        print("Runtime: {:.2f} s".format(curr_runtime))
        print("Cost: {:.2f} units".format(curr_cost))

def main():
    for ds in DATA_SIZE:
        for query in QUERIES:
            # Get application average runtimes from csv file
            query_avg_runtimes = get_query_avg_runtime(query, ds)
            # Get application average runtimes and costs when fixing the number of executors
            get_runtimes_and_costs_fix_nexs(query_avg_runtimes)
            print("Getting optimal configuration for {} query with data size {}..".format(query, ds))
            # Extract application runtime plot
            fix_runtime, fix_cost, opt_nexs = get_app_opt_nexs(query_avg_runtimes)
            print("Query optimal number of executors: {}".format(opt_nexs))
            print("Corresponding Runtime: {:.2f} s".format(fix_runtime))
            print("Corresponding Cost: {:.2f} units".format(fix_cost))
            # Get stages data from csv file 
            stages_data = get_stages_avg_runtimes(query, ds)
            # Extract stages runtime plots
            runtime, cost, opt_conf = get_conf_opt_nexs(stages_data, ds, query)
            print("Optimal configuration:")
            print(opt_conf)
            print("Corresponding Runtime: {:.2f} s".format(runtime))
            print("Corresponding Cost: {:.2f} units".format(cost))

if __name__ == "__main__":
    main()

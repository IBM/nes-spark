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
BENCHMARK = "tpch"

QUERIES = [
    "q9"
#    "q14a",
#    "q67",
#    "q72"
#    "q95"
#    "q23a"
]

def get_app_data(query, ds):
    file = open('files/' + BENCHMARK + '/' + query + '_sf' + str(ds) + '_application.csv')
    csvreader = csv.reader(file)
    data = []
    data_errs = []
    for row in csvreader:
        # Row information: [num_exs, app_avgruntime, app_medruntime, app_min_runtime, app_max_runtime]
        data.append(row)        
    file.close()
    return data 

def get_app_average_runtimes_plot(query, ds, data):
    app_exs_points = []
    app_exs_minerrs = []
    app_exs_maxerrs = []
    # Get application medians
    for idx_exs in range(len(EXECUTORS)):
        app_exs_points.append(float(data[idx_exs][1]))
        app_exs_minerrs.append(float(data[idx_exs][1]) - float(data[idx_exs][3]))
        app_exs_maxerrs.append(float(data[idx_exs][4]) - float(data[idx_exs][1]))
    app_exs_errs = [app_exs_minerrs, app_exs_maxerrs]
    legend_label = BENCHMARK.split('-')[0].upper() + ' ' + query
    plt.figure()
    plt.errorbar(EXECUTORS, app_exs_points, yerr=app_exs_errs, color='black', linestyle='', elinewidth=1, capsize=5)
    plt.plot(EXECUTORS, app_exs_points, color = 'darkblue', linestyle = 'solid', marker = 's', label = legend_label)
    plt.xlabel('Number of executors')
    plt.xticks(EXECUTORS)
    plt.ylabel('Average Runtime [s]')
    plt.ylim(ymin=0, ymax=250)
    #plt.ylim([0,300])
    plt.legend()
    if not os.path.exists('plots/' + BENCHMARK + '/' + query + '_sf' + str(ds)):
        os.makedirs('plots/' + BENCHMARK + '/' + query + '_sf' + str(ds))
    plt.savefig('plots/' + BENCHMARK + '/' + query + '_sf' + str(ds) + '/' + BENCHMARK + '-' + query + '-sf' + str(ds) + '.pdf')
    plt.clf()

def get_stages_data(query, ds):
    file = open('files/' + BENCHMARK + '/' + query + '_sf' + str(ds) + '_stages.csv')
    csvreader = csv.reader(file)
    tmp_num_exs = EXECUTORS[0]
    exs_data = []
    data = []
    for row in csvreader:
        # Row information: [num_exs, job_id, stage_id, stage_avgruntime, stage_tasks_acctime, stage_medruntime, stage_min_runtime, stage_max_runtime]
        if int(row[0]) == tmp_num_exs:
            exs_data.append([int(row[2])] + [float(item) for item in row[3:]])
        else:
            data.append(exs_data)
            tmp_num_exs = int(row[0])
            exs_data = []
            exs_data.append([int(row[2])] + [float(item) for item in row[3:]])
    data.append(exs_data)
    file.close()
    return data 

def get_stages_avg_runtimes_plots(query, ds, data):
    stages_points = []
    stages_errs = []
    stages_ids = []
    # Get stages averages
    for idx_stage, stage in enumerate(data[0]):
        stages_ids.append(stage[0])
        stage_points = []
        stage_errs = []
        stage_minerrs = []
        stage_maxerrs = []
        for idx_exs in range(len(EXECUTORS)):
            stage_points.append(data[idx_exs][idx_stage][1])
            stage_minerrs.append(data[idx_exs][idx_stage][1] - data[idx_exs][idx_stage][4])
            stage_maxerrs.append(data[idx_exs][idx_stage][5] - data[idx_exs][idx_stage][1])
        stages_points.append(stage_points)
        stage_errs = [stage_minerrs, stage_maxerrs]
        stages_errs.append(stage_errs)
    if not os.path.exists('plots/' + BENCHMARK + '/' + query + '_sf' + str(ds)):
        os.makedirs('plots/' + BENCHMARK + '/' + query + '_sf' + str(ds))
    for stage_id, stage_points, stage_errs in zip(stages_ids, stages_points, stages_errs):
        legend_label = 'Stage ' + str(stage_id)
        plt.figure(1)
        plt.errorbar(EXECUTORS, stage_points, stage_errs, color='black', linestyle='', elinewidth=1, capsize=5)
        plt.plot(EXECUTORS, stage_points, color = 'g', linestyle = 'solid', marker = 'o', label = legend_label)
        plt.xlabel('Number of executors')
        plt.xticks(EXECUTORS)
        plt.ylabel('Average Runtime [s]')
        plt.ylim(ymin=0)
        plt.legend()
        plt.savefig('plots/' + BENCHMARK + '/' + query + '_sf' + str(ds) + '/' + BENCHMARK + '-' + query + '-sf' + str(ds) + '-stage' + str(stage_id) + '.pdf')
        plt.clf()

def get_single_stage_cost_performance_plot(query, ds, stage_id, stages_data):
    stage_points = []
    stage_errs = []
    stage_minerrs = []
    stage_maxerrs = []
    stages_ids = [s[0] for s in stages_data[0]]
    idx_stage = stages_ids.index(stage_id)
    for idx_exs in range(len(EXECUTORS)):
        stage_points.append(stages_data[idx_exs][idx_stage][1])
        stage_minerrs.append(stages_data[idx_exs][idx_stage][1] - stages_data[idx_exs][idx_stage][4])
        stage_maxerrs.append(stages_data[idx_exs][idx_stage][5] - stages_data[idx_exs][idx_stage][1])
    stage_errs = [stage_minerrs, stage_maxerrs]
    costs = []
    for idx_exs, point in enumerate(stage_points):
        costs.append(point * EXECUTORS[idx_exs])
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.errorbar(EXECUTORS, stage_points, stage_errs, color='black', linestyle='', elinewidth=1, capsize=5)
    ax1.plot(EXECUTORS, stage_points, color = 'darkgreen', linestyle = 'solid', marker = 'o', label = 'Stage ' + str(stage_id))
    ax1.legend(loc=0, bbox_to_anchor=(+10,1),fancybox=False, shadow=False)
    ax2.plot(EXECUTORS, costs, color = 'tomato', linestyle = 'solid', marker = 'x', label = 'Stage ' + str(stage_id))
    ax2.legend(loc=0, bbox_to_anchor=(-10, -0.2),fancybox=False, shadow=False)
    ax1.set_xlabel("Number of Executors")
    ax1.set_xticks(EXECUTORS) 
    ax1.set_ylabel('Average Runtime [s]')
    ax2.set_ylabel('Cost [units]')
    ax1.set_ylim(bottom=0)
    ax2.set_ylim(bottom=0)
    plt.savefig('plots/' + BENCHMARK + '/' + query + '/' + BENCHMARK + '-' + query + '-sf' + str(ds) + '-stage' + str(stage_id) + 'costperf.pdf')
    plt.clf()

def get_single_stage_speedup_plot(query, stage_id, stages_data):
    stage_speedups = []
    stages_ids = [s[0] for s in stages_data[0]]
    idx_stage = stages_ids.index(stage_id)
    baseline = stages_data[0][idx_stage][1]
    for idx_exs in range(len(EXECUTORS)):
        stage_speedups.append(baseline/stages_data[idx_exs][idx_stage][1])
    plt.figure(1)
    plt.plot(EXECUTORS, stage_speedups, color = 'teal', linestyle = 'solid', marker = 'D', label = 'Stage ' + str(stage_id))
    plt.plot(EXECUTORS, EXECUTORS, color = 'orange', linestyle = 'solid', marker = 'D', label = 'Ideal (Amdahl\'s Law)')
    plt.xlabel('Number of executors')
    plt.xticks(EXECUTORS)
    plt.ylabel('Avg. Runtime Speedup')
    plt.ylim(ymin=0)
    plt.legend()
    plt.savefig('plots/' + BENCHMARK + '/' + query + '/' + BENCHMARK + '-' + query + '-sf10-stage' + str(stage_id) + 'speedups.pdf')
    plt.clf()

def get_single_stage_cost_performance_ratio_plot(query, stage_id, stages_data):
    stage_ratios= []
    stages_ids = [s[0] for s in stages_data[0]]
    idx_stage = stages_ids.index(stage_id)
    baseline_time = stages_data[0][idx_stage][1]
    baseline_cost =  stages_data[0][idx_stage][1]
    for idx_exs in range(1, len(EXECUTORS)):
        speedup = baseline_time/stages_data[idx_exs][idx_stage][1]
        cost_inc = (stages_data[idx_exs][idx_stage][1] * EXECUTORS[idx_exs])/baseline_cost
        stage_ratios.append(cost_inc/speedup)
    plt.figure(1)
    bars = plt.bar(EXECUTORS[1:], stage_ratios, color ='gold', width = 4)
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x()-1.5, yval + .007, round(yval,3))
    plt.xlabel('Number of executors')
    plt.xticks(EXECUTORS)
    plt.ylabel('Avg. Cost Performance Ratio')
    plt.ylim(ymin=0)
    plt.savefig('plots/' + BENCHMARK + '/' + query + '/' + BENCHMARK + '-' + query + '-sf10-stage' + str(stage_id) + 'costperfratio.pdf')
    plt.clf()

def get_single_stage_performance_pairs_plot(query, stage_id, stages_data):
    stage_cost_diffs = []
    stage_time_diffs = []
    stages_ids = [s[0] for s in stages_data[0]]
    idx_stage = stages_ids.index(stage_id)
    baseline_time = stages_data[0][idx_stage][1]
    baseline_cost =  stages_data[0][idx_stage][1]
    xticks = []
    for p_idx_val, val in enumerate(EXECUTORS[1:]):
        xticks.append(str(EXECUTORS[p_idx_val]) + '-' + str(EXECUTORS[p_idx_val+1]))
    for idx_exs in range(1, len(EXECUTORS)):
        time_diff = stages_data[idx_exs-1][idx_stage][1] - stages_data[idx_exs][idx_stage][1]
        cost_diff = (stages_data[idx_exs][idx_stage][1] * EXECUTORS[idx_exs]) - (stages_data[idx_exs-1][idx_stage][1] * EXECUTORS[idx_exs-1])
        stage_time_diffs.append(time_diff)
        stage_cost_diffs.append(cost_diff)
    fig, axs = plt.subplots(2)
    axs[0].bar([x for x in range(1, len(EXECUTORS))], stage_time_diffs, color ='darkgreen', width = 0.5)
    axs[1].bar([x for x in range(1, len(EXECUTORS))], stage_cost_diffs, color ='orange', width = 0.5)
    axs[1].set_xlabel('Number of executors')
    axs[1].set_xticks([x for x in range(1, len(EXECUTORS))], xticks)
    axs[0].set_xticks([x for x in range(1, len(EXECUTORS))], xticks)
    axs[0].set_xticklabels([])
    axs[0].set_ylabel('Avg. Time Variation [s]')
    axs[1].set_ylabel('Cost Increase [units]')
    axs[1].set_ylim(bottom=0)
    axs[0].axhline(y=0, c="red",linewidth=0.7,zorder=0)
    axs[0].set_ylim(bottom=-1)
    plt.savefig('plots/' + BENCHMARK + '/' + query + '/' + BENCHMARK + '-' + query + '-sf10-stage' + str(stage_id) + 'perfcostpairs.pdf')
    plt.clf()

def main():
    for ds in DATA_SIZE:
        for query in QUERIES:
            print("Getting plots for {} query with data size {}..".format(query, ds))
            # Get application data from csv file
            app_data = get_app_data(query, ds)
            # Extract application runtime plot
            get_app_average_runtimes_plot(query, ds, app_data)
            # Get stages data from csv file 
            stages_data = get_stages_data(query, ds)
            # Extract stages runtime plots
            get_stages_avg_runtimes_plots(query, ds, stages_data)
            # Extract single stage cost-performance plot
            #get_single_stage_cost_performance_plot(query, ds, 26, stages_data)
            # Extract single stage speedup plot
            #get_single_stage_speedup_plot(query, 26, stages_data)
            #get_single_stage_cost_performance_ratio_plot(query, 26, stages_data)
            #get_single_stage_performance_pairs_plot(query, 26, stages_data)

if __name__ == "__main__":
    main()

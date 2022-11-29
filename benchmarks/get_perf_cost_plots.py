import os
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable
import csv

# Define benchmark name (either tpch or tpcds-sql
BENCHMARK_NAME = 'tpcds-sql'

# Define list of number of executors
EXECUTORS = [ 1,
        10,
        20,
        30
]

# Define list of queries to consider
QUERIES = [ "q72" 
]

# Define list of input data sizes
DATA_SIZE = [ 100 
]


def get_perf_cost_plot(query, ds, f_exs_runtimes_costs, d_exs_runtimes_costs, opt_steps_runtimes_costs):
    fig, (ax1, ax2) = plt.subplots(nrows=2, sharex=True)
    exs_colors = [ 'royalblue',
            'firebrick',
            'darkorange',
            'gold'
    ]
    for idx_ex, ex in enumerate(f_exs_runtimes_costs):
        ax1.axhline(y = ex[0],
                xmin=0.0,
                xmax=1.0,
                c = exs_colors[idx_ex],
                linewidth=1.5,
                linestyle = 'solid',
                label = 'Fixed' + str(EXECUTORS[idx_ex])
        )
        ax2.axhline(y = ex[1],
                xmin=0.0,
                xmax=1.0,
                c = exs_colors[idx_ex],
                linewidth=1.5,
                linestyle = 'solid'
        )
    for idx_d_ex, d_ex in enumerate(d_exs_runtimes_costs):
        ax1.axhline(y = d_ex[0],
                xmin=0.0,
                xmax=1.0,
                c = exs_colors[idx_d_ex+1],
                linewidth=1.5,
                linestyle = 'dashed',
                label = 'Dyn' + str(EXECUTORS[idx_d_ex+1])
        )
        ax2.axhline(y = d_ex[1],
            xmin=0.0,
            xmax=1.0,
            c = exs_colors[idx_d_ex+1],
            linewidth=1.5,
            linestyle = 'dashed'
        )
    steps_idx = range(1, len(opt_steps_runtimes_costs) + 1)
    steps_runtimes = [step[0] for step in opt_steps_runtimes_costs]
    steps_costs = [step[1] for step in opt_steps_runtimes_costs]
    ax1.plot(steps_idx, steps_runtimes, "-o", color = 'forestgreen', label = 'Optimized')
    ax2.plot(steps_idx, steps_costs, "-o", color = 'forestgreen')
    ax1.set_ylabel('Runtime [s]')
    #ax1.legend(ncol=1, loc='center left', bbox_to_anchor=(1, -0.15))
    ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.35), ncol=4)
    ax1.set_xticks([x for x in range(1, len(opt_steps_runtimes_costs) + 1)])
    ax1.set_xlim(0, len(opt_steps_runtimes_costs) + 1)
    ax2.set_xticks([x for x in range(1, len(opt_steps_runtimes_costs) + 1)])
    ax2.set_xlim(0, len(opt_steps_runtimes_costs) + 1)
    ax2.set_xlabel('Optimization Step')
    ax2.set_ylabel('Cost [unit]')
    fig_folder = 'plots/' + BENCHMARK_NAME + '/' + query + '_sf' + str(ds) + '/'
    if not os.path.exists(fig_folder):
        os.makedirs(fig_folder)
    plt.savefig(fig_folder + BENCHMARK_NAME + '-' + query + '-sf' + str(ds) + '-perf-cost-plot.pdf')
    plt.close(fig)

def read_fix_f_exs_runtimes_costs(ds, query):
    file_app = open('files/' + BENCHMARK_NAME + '/' + query + '_sf' + str(ds) + '_application.csv')
    file_exs = open('files/' + BENCHMARK_NAME + '/' + query + '_sf' + str(ds) + '_exs_times.csv')
    app_csvreader = csv.reader(file_app)
    exs_csvreader = csv.reader(file_exs)
    f_exs_runtimes_costs = []
    count = 0 
    for row_app, row_exs in zip(app_csvreader, exs_csvreader):
        f_exs_runtimes_costs.append([float(row_app[1]), float(row_exs[1])/1000])
        print("With {} fixed executors: runtime {} cost {} s".format(EXECUTORS[count], row_app[1], float(row_exs[1])/1000))
        count += 1
        if count == len(EXECUTORS):
            break
    file_app.close()
    file_exs.close()
    return f_exs_runtimes_costs

def read_dyn_f_exs_runtimes_costs(ds, query):
    file_app = open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_dyn_application.csv")
    file_exs = open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_dyn_exs_times.csv")
    app_csvreader = csv.reader(file_app)
    exs_csvreader = csv.reader(file_exs)
    d_exs_runtimes_costs = []
    count = 0
    for row_app, row_exs in zip(app_csvreader, exs_csvreader):
        d_exs_runtimes_costs.append([float(row_app[2]), float(row_exs[2])/1000])
        print("With {} dynamic executors: runtime {} cost {} s".format(EXECUTORS[count+1], row_app[2], float(row_exs[2])/1000))
        count += 1
        if count == len(EXECUTORS) - 1:
            break
    file_app.close()
    file_exs.close()
    return d_exs_runtimes_costs

def read_opt_steps_runtimes_costs_runtimes(ds, query):
    file = open("files/" + BENCHMARK_NAME + '/' + query + "_sf" + str(ds) + "_opt_confs.csv")
    csvreader = csv.reader(file)
    opt_runtimes_costs = []
    for row in csvreader:
        opt_runtimes_costs.append([float(row[0]), float(row[1])])
    file.close()
    return opt_runtimes_costs

def main():
    for ds in DATA_SIZE:
        for query in QUERIES:
            # Get overall pipeline runtimes and costs with a fixed number of executors
            f_exs_runtimes_costs = read_fix_f_exs_runtimes_costs(ds, query)
            # Get overall pipeline runtimes and costs with a dynamic number of executors
            d_exs_runtimes_costs = read_dyn_f_exs_runtimes_costs(ds, query)
            # Get optimal pipeline runtime and cost through different optimization steps
            opt_steps_runtimes_costs = read_opt_steps_runtimes_costs_runtimes(ds, query)
            # Extract performance vs cost plot
            get_perf_cost_plot(query, ds, f_exs_runtimes_costs, d_exs_runtimes_costs, opt_steps_runtimes_costs)

if __name__ == "__main__":
    main()

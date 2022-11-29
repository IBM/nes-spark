from typing import Optional

import argparse

import pyarrow.parquet as pq
import pyarrow.csv as csv
from pyarrow import fs as pyarrow_fs
from pydantic import BaseModel, HttpUrl, AnyUrl
import re
import sys 

# Class defining the minion configurations
class S3ServiceConfig(BaseModel):
    access_key: str
    secret_key: str
    endpoint_url: Optional[HttpUrl]


# Minio configurations
zac_config = S3ServiceConfig(
    access_key='',
    secret_key="",
    endpoint_url=""
)

# Open remote filesystem 
def open_remote_filesystem(config: S3ServiceConfig):
    return pyarrow_fs.S3FileSystem(access_key=config.access_key, secret_key=config.secret_key,
                                  endpoint_override=config.endpoint_url)

# Read a table from remote filesystem
def read_table(source: AnyUrl, fs: pyarrow_fs.S3FileSystem):
    return pq.read_table(source, fs)

def compare_all_queries(benchmark, reference_folder: str, test_folder: str, reference_filesystem: Optional[pyarrow_fs.FileSystem], test_filesystem: Optional[pyarrow_fs.FileSystem]):
    # Get all runs in test forlder
    runs = test_filesystem.get_file_info(pyarrow_fs.FileSelector(test_folder))
    if benchmark == 'tpcds':
        for i in range(1, 100):
            query = 'q' + str(i) +'.sql'
            query_runs_paths = [run.path for run in runs if query.split('.')[0] + '-'  in run.path and 'sf100' in run.path]
            for qrp in query_runs_paths:
                compare_tpcds_single_query(qrp, query, reference_folder, reference_filesystem, test_filesystem)
    else:
        for i in range(1,23):
            query = 'q' + str(i) +'.sql'
            query_runs_paths = [run.path for run in runs if query.split('.')[0] + '-' in run.path and 'sf1' in run.path]
            for qrp in query_runs_paths:
                compare_tpch_single_query(qrp, query.split('.')[0], reference_folder, reference_filesystem, test_filesystem)

def compare_queries_subset(benchmark, set_queries, reference_folder: str, test_folder: str, reference_filesystem: Optional[pyarrow_fs.FileSystem], test_filesystem: Optional[pyarrow_fs.FileSystem]):
    # Get all runs in test folder
    runs = test_filesystem.get_file_info(pyarrow_fs.FileSelector(test_folder))
    for query in set_queries:
        # Get all runs for the specific query
        if benchmark =='tpcds':
            query_runs_paths = [run.path for run in runs if query.split('.')[0] + '-' in run.path and 'sf100' in run.path]
        else:
            query_runs_paths = [run.path for run in runs if query.split('.')[0] + '-' in run.path and 'sf1' in run.path]
        # Check correctness of all the runs
        for qrp in query_runs_paths:
            if benchmark == 'tpcds':
                compare_tpcds_single_query(qrp, query, reference_folder, reference_filesystem, test_filesystem)
            else:
                compare_tpch_single_query(qrp, query.split('.')[0], reference_folder, reference_filesystem, test_filesystem)

def compare_tpcds_single_query(run_folder: str, query: str, reference_folder: str, reference_filesystem: Optional[pyarrow_fs.FileSystem], test_filesystem: Optional[pyarrow_fs.FileSystem]):
    # Get reference and test tables
    reference = pq.read_table(f"{reference_folder}/{query}/", filesystem=reference_filesystem)
    test = pq.read_table(f"{run_folder}/", filesystem=test_filesystem)
    # Check correctness of the test outcome
    result = reference.equals(test)
    run = run_folder.split('/')[3]
    print(f"{query} run {run}: {result}")
    if not result:
        print("ERROR: the test outcome is different from the reference!")
        print("Reference: ")
        print(refeference.to_pydict())
        print("Test: ")
        print(test.to_pydict())

def compare_tpch_single_query(run_folder: str, query: str, reference_folder: str, reference_filesystem: Optional[pyarrow_fs.FileSystem], test_filesystem: Optional[pyarrow_fs.FileSystem]):
    # Get reference and test tables
    #print(reference_folder)
    reference = pq.read_table(f"{reference_folder}/{query}.parquet", filesystem=reference_filesystem)
    ref_dict = reference.to_pydict()
    test =  pq.read_table(f"{run_folder}/", filesystem=test_filesystem)
    test_dict = test.to_pydict()
    # Check correctness of the test outcome (done on py dict due to int64 non null types in parquet tables)
    result = ref_dict == test_dict
    run = run_folder.split('/')[3]
    print(f"{query} run {run}: {result}")
    if not result:
        print("ERROR: the test outcome is different from the reference!")
        print("Reference: ")
        print(ref_dict)
        print("Test: ")
        print(test_dict)

if __name__ == "__main__":
    parser = argparse.ArgumentParser() 
    parser.add_argument('-b', '--benchmark', dest='benchmark', required=True, choices=['tpcds', 'tpch'])
    args = parser.parse_args()
    
    # Define subset of queries in the form 'q1.sql' 
    subset = [  
        ]

    # Define reference tpcds benchmark folder and test folder
    if args.benchmark == 'tpcds':
        reference_folder = "tpc-ds/reference_tpc-ds100/100"
        if len(subset) > 0:
            test_folder = "nes/outputs/sql"
        else:
            test_folder = "nes/outputs/tpcds"
    else:
        reference_folder = "tpc-h/reference"
        test_folder = "nes/outputs/tpch"
    # Open remote file systems 
    reference_fs = open_remote_filesystem(zac_config)
    test_fs = open_remote_filesystem(zac_config)
    # Test correctness of tests outputs
    if len(subset) > 0:
        compare_queries_subset(args.benchmark, subset, reference_folder, test_folder, reference_filesystem=reference_fs, test_filesystem=test_fs)
    else:
        compare_all_queries(args.benchmark, reference_folder, test_folder, reference_filesystem=reference_fs, test_filesystem=test_fs)

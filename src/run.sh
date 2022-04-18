#!/bin/bash

declare -a frameworks=("spark" "dask")
declare -a tasks=("load_write" "filter" "group_by" "join" "self_join" "sort")
declare -a data_sizes=("100k" "1M" "5M" "10M")
declare -a file_types=("parquet")
it=10

for framework in "${frameworks[@]}"
do
    for task in "${tasks[@]}"
    do
        for size in "${data_sizes[@]}"
        do
            for file_type in "${file_types[@]}"
            do
                for ((i=0; i < $it; i++)); 
                do
                    python3 run.py $framework $task $size $file_type
                done
            done
        done
    done
done
#!/bin/sh
BENCHMARK_TIMES=$1
LOOP_TIMES=$2 


REPORT_NAME="csharp_run_${BENCHMARK_TIMES}_loop_${LOOP_TIMES}"
RESULT_FOLDER="result"

echo "===== start ..."
if [ ! -d "result" ]
then
    mkdir ${RESULT_FOLDER}
fi
echo "BENCHMARK_TIMES:${BENCHMARK_TIMES} LOOP_TIMES:${LOOP_TIMES}"

echo "===== prepare ..."
dotnet run -- -s prepare
dotnet run -- -s prepare -t json

# if need to run taosBenchmark
# run taosbench mark

echo "==== run benchmark"
hyperfine -r ${BENCHMARK_TIMES} -L stage connect,insert,batch,batchcol,query,avg -L times ${LOOP_TIMES} -L types normal,json 'dotnet run -- -s {stage} -n {times} -t {types}' --time-unit millisecond  --show-output --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}.md --command-name {stage}_{types}

echo "==== cleanup ..."
dotnet run -- -s clean

echo "=== benchmark done ... "
echo "=== result file:${RESULT_FOLDER}/${REPORT_NAME}.md "
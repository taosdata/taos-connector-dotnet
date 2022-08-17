#!/bin/sh
BENCHMARK_TIMES=$1
 
REPORT_NAME="csharp_run_${BENCHMARK_TIMES}"
RESULT_FOLDER="result"
 
echo "===== start ..."
if [ ! -d "result" ]
then
    mkdir ${RESULT_FOLDER}
fi
echo "BENCHMARK_TIMES:${BENCHMARK_TIMES}"

# clear remaining result report
# rm -rf ./${RESULT_FOLDER}/*.md
 
echo "===== step 1 create tables ..."
taosBenchmark -f ./data/only_create_table_with_normal_tag.json
taosBenchmark -f ./data/only_create_table_with_json_tag.json

echo "===== step 2 insert data..."
hyperfine -r ${BENCHMARK_TIMES} -L types normal,json -L tables 100 \
 'dotnet run -- -s insert  -t {types} -b {tables}' \
 --time-unit millisecond  \
 --show-output \
 --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}_insert.md \
 --command-name insert_{types}_${BENCHMARK_TIMES}

echo "===== step 3 clean data and create tables ..."
# taosBenchmark -f ./data/only_create_table_with_normal_tag.json
# taosBenchmark -f ./data/only_create_table_with_json_tag.json
 
echo "===== step 4 insert data with batch ..."
 
echo "===== step 5 query..."
hyperfine -r ${BENCHMARK_TIMES} -L types normal,json -L tables 100 \
 'dotnet run -- -s query  -t {types} ' \
 --time-unit millisecond  \
 --show-output \
 --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}_query.md \
 --command-name insert_{types}_${BENCHMARK_TIMES}

echo "===== step 6 avg ..."
hyperfine -r ${BENCHMARK_TIMES} -L types normal,json -L tables 100 \
 'dotnet run -- -s avg -t {types}' \
 --time-unit millisecond  \
 --show-output \
 --export-markdown ${RESULT_FOLDER}/${REPORT_NAME}_avg.md \
 --command-name avg_{types}_${BENCHMARK_TIMES}
  

echo "| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |">>./${RESULT_FOLDER}/${REPORT_NAME}.md
echo "|:---|---:|---:|---:|---:|">>./${RESULT_FOLDER}/${REPORT_NAME}.md
ls ./${RESULT_FOLDER}/*.md|
while read filename;
do
    sed -n '3,4p' ${filename}>>${RESULT_FOLDER}/${REPORT_NAME}.md
    #echo "">>${RESULT_FOLDER}/${REPORT_NAME}.md
done 

echo "=== benchmark done ... "
echo "=== result file:${RESULT_FOLDER}/${REPORT_NAME}.md "
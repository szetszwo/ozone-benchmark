# Ozone Benchmark
for comparing the performance of *Stream* and *Async* write pipeline.

- Compile
```shell
export MVN_SKIP="-DskipTests -DskipDocs -Dmaven.javadoc.skip=true"
export MVN_SETTINGS="/path/to/setting.xml"

mvn -s $MVN_SETTINGS clean package $MVN_SKIP
```

- Set Classpath
```shell
cd $BENCHMARK_HOME
export CLASSPATH=`mvn -s $MVN_SETTINGS exec:exec | grep ${PWD}/`
```

- Run
```shell
OM=...                  # OM address
CLIENTS=...             # a comma separated client list
BENCHMARK_ID=`hostname` # must be distinct for multiple clients.
TYPE=STREAM             # or ASYNC or STREAM_API_BYTE_ARRAY 
FILE_NUM=100
FILE_SIZE=128m
CHUNK_SIZE=8m
THREAD_NUM=32

DATETIME=`date +%Y%m%d-%H%M`; \
F=output-3clients_${FILE_NUM}x${FILE_SIZE}_${TYPE}_c${CHUNK_SIZE}_t${THREAD_NUM}_${DATETIME}.txt; \
  java -Xms64g -Xmx64g org.apache.hadoop.ozone.benchmark.Benchmark \
  -om ${OM} -localDirs ${LOCAL_ROOT}/benchmark-dir/ \
  -verify \
  -type ${TYPE} -id ${BENCHMARK_ID} -threadNum ${THREAD_NUM} \
  -fileNum ${FILE_NUM} -fileSize ${FILE_SIZE} -chunkSize ${CHUNK_SIZE} \
  -clients ${CLIENTS} \ 
  | tee ${F}
```

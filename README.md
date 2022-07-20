# Ozone Benchmark
for comparing the performance of *Stream*, *Async* and *HDFS* write pipeline.

- Compile
```shell
export MVN_SKIP="-DskipTests -DskipDocs -Dmaven.javadoc.skip=true"
export MVN_SETTINGS="/path/to/setting.xml"

mvn -s $MVN_SETTINGS clean package $MVN_SKIP
```

- Set classpath for Ozone only
```shell
cd $BENCHMARK_HOME
export CLASSPATH=`mvn -s $MVN_SETTINGS exec:exec | grep ${PWD}/`
```
- Set classpath for Ozone and HDFS
```shell
cd $BENCHMARK_HOME
export CLASSPATH=`hdfs classpath`:`mvn -s $MVN_SETTINGS exec:exec | grep ${PWD}/`
```

- Run
```shell
SERVICE_ADDRESS=...     # OM address or NN address
CLIENTS=...             # a comma separated client list
BENCHMARK_ID=`hostname` # must be distinct for multiple clients.
TYPE=STREAM             # or ASYNC or STREAM_API_BYTE_ARRAY or HDFS
FILE_NUM=100
FILE_SIZE=128m
CHUNK_SIZE=8m
THREAD_NUM=32

DATETIME=`date +%Y%m%d-%H%M`; \
NUM_CLIENTS=`echo ${CLIENTS} | sed -e "s/,/ /g" | sed -e "s/;/ /g" | wc -w`; \
if [ ${NUM_CLIENTS} -eq 0 ] ; then NUM_CLIENTS=1;  fi ; echo $NUM_CLIENTS; \
F=output-${NUM_CLIENTS}c_${FILE_NUM}x${FILE_SIZE}_${TYPE}_c${CHUNK_SIZE}_t${THREAD_NUM}_${DATETIME}.txt; \
  java -Xms64g -Xmx64g org.apache.hadoop.ozone.benchmark.Benchmark \
  -serviceAddress ${SERVICE_ADDRESS} -localDirs ${LOCAL_ROOT}/benchmark-dir/ \
  -verify \
  -type ${TYPE} -id ${BENCHMARK_ID} -threadNum ${THREAD_NUM} \
  -fileNum ${FILE_NUM} -fileSize ${FILE_SIZE} -chunkSize ${CHUNK_SIZE} \
  -clients ${CLIENTS} \ 
  | tee ${F} ; \
echo ${F}
```

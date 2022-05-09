/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.benchmark;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Performance Benchmark
 */
public class Benchmark {
  enum Op {PREPARE_LOCAL_FILES, INIT_WRITER}

  interface Parameters {
    String getType();

    String getId();

    String getOm();

    int getFileNum();

    String getFileSize();

    String getChunkSize();

    String getLocalDirs();

    boolean isDropCache();

    int getThreadNum();

    default String getSummary() {
      return getType() + "[" + getFileNum() + " x " + getFileSize() + "]";
    }
  }

  enum Type {
    ASYNC_API(AsyncWriter::new),
    STREAM_API(StreamWriter::new);

    private final Function<List<String>, Writer> constructor;

    Type(Function<List<String>, Writer> constructor) {
      this.constructor = constructor;
    }

    Writer newWrite(List<String> paths) {
      return constructor.apply(paths);
    }

    static Type parse(String s) {
      for (Type t : values()) {
        if (t.name().startsWith(s.trim().toUpperCase(Locale.ENGLISH))) {
          return t;
        }
      }
      throw new IllegalArgumentException("Failed to parse " + s);
    }
  }

  static OzoneClient getOzoneClient(String omAddress, SizeInBytes chunkSize) throws IOException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.address", omAddress);
    conf.set("ozone.client.checksum.type", "NONE");
    conf.set("ozone.client.datastream.buffer.flush.size", "1GB");
    conf.set("ozone.client.datastream.window.size", "1GB");
    conf.set("ozone.client.datastream.min.packet.size", chunkSize.getSize() + "B");
    conf.set("hdds.ratis." + NettyConfigKeys.DataStream.Client.WORKER_GROUP_SHARE_KEY, "true");
    conf.set("hdds.ratis." + NettyConfigKeys.DataStream.Client.WORKER_GROUP_SIZE_KEY, "100");
    return OzoneClientFactory.getRpcClient(conf);
  }

  private final String id;
  private final Parameters parameters;
  private final SizeInBytes fileSize;
  private final SizeInBytes chunkSize;

  Benchmark(Parameters parameters) {
    this.id = Optional.ofNullable(parameters.getId()).filter(id -> !id.isEmpty()).orElseGet(Print::randomId);
    this.parameters = parameters;
    this.fileSize = SizeInBytes.valueOf(parameters.getFileSize());
    this.chunkSize = SizeInBytes.valueOf(parameters.getChunkSize());
  }

  static List<String> prepareLocalFiles(String id, int fileNum, SizeInBytes fileSize, SizeInBytes chunkSize,
      String localDirsString, boolean isDropCache) throws Exception {
    Print.ln(Op.PREPARE_LOCAL_FILES, id + ": fileNum=" + fileNum + ", fileSize=" + fileSize);

    final List<LocalDir> localDirs = Utils.parseFiles(localDirsString).stream()
        .map(dir -> new File(dir, id))
        .map(LocalDir::new)
        .collect(Collectors.toList());
    try {
      Utils.createDirs(Utils.transform(localDirs, LocalDir::getPath));

      final List<String> paths = Utils.generateLocalFiles(localDirs, fileNum, fileSize, chunkSize);
      if (isDropCache) {
        Utils.dropCache(fileSize, fileNum, localDirs.size());
      }
      return paths;
    } finally {
      localDirs.forEach(LocalDir::close);
    }
  }

  OzoneVolume initVolume(ObjectStore store) throws IOException {
    final String volumeName = "bench-vol-" + id;
    try {
      final OzoneVolume exists = store.getVolume(volumeName);
      if (exists != null) {
        Print.ln(Op.INIT_WRITER, "Volume " + volumeName + " already exists.");
        return exists;
      }
    } catch (OMException e) {
    }
    store.createVolume(volumeName);
    Print.ln(Op.INIT_WRITER, "Volume " + volumeName + " created.");
    return store.getVolume(volumeName);
  }

  OzoneBucket initBucket(OzoneVolume volume) throws IOException {
    final String bucketName = "bench-buck-" + id;
    try {
      final OzoneBucket exists = volume.getBucket(bucketName);
      if (exists != null) {
        Print.ln(Op.INIT_WRITER, "Bucket " + bucketName + " already exists.");
        return exists;
      }
    } catch (OMException e) {
    }
    volume.createBucket(bucketName);
    Print.ln(Op.INIT_WRITER, "Bucket " + bucketName + " created.");
    return volume.getBucket(bucketName);
  }

  static String deleteKeyIfExists(int i, OzoneBucket bucket) throws IOException {
    final String key = String.format("key_%04d", i);
    OzoneKeyDetails exists = null;
    try {
      exists = bucket.getKey(key);
    } catch (OMException e) {
    }
    if (exists != null) {
      Print.ln(Op.INIT_WRITER, "Key " + key + " already exists; deleting it...");
      bucket.deleteKey(key);
    }
    return key;
  }

  @FunctionalInterface interface CreateKey {
    OzoneOutputStream apply(int i) throws IOException;
  }

  Writer initWriter(List<String> paths, OzoneClient ozoneClient) throws IOException {
    Print.ln(Op.INIT_WRITER, "OzoneClient " + ozoneClient);
    // An Ozone ObjectStore instance is the entry point to access Ozone.
    final ObjectStore store = ozoneClient.getObjectStore();
    Print.ln(Op.INIT_WRITER, "Store " + store.getCanonicalServiceName());

    // Create volume.
    final OzoneVolume volume = initVolume(store);
    // Create bucket with random name.
    final OzoneBucket bucket = initBucket(volume);
    final ReplicationConfig replication = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

    final Type type = Type.parse(parameters.getType());
    Print.ln(Op.INIT_WRITER, "Type " + type);
    final Writer writer = type.newWrite(paths).init(fileSize.getSize(), replication, bucket);
    Print.ln(Op.INIT_WRITER, writer);
    return writer;
  }

  void run(Sync.Server launchSync) throws Exception {
    final List<String> paths = prepareLocalFiles(id, parameters.getFileNum(), fileSize, chunkSize,
        parameters.getLocalDirs(), parameters.isDropCache());
    final ExecutorService executor = Executors.newFixedThreadPool(parameters.getThreadNum());
    try(OzoneClient ozoneClient = getOzoneClient(parameters.getOm(), chunkSize)) {
      final Writer writer = initWriter(paths, ozoneClient);

      // wait for sync signal
      launchSync.readyAndWait(true);

      final Instant start = Instant.now();
      // Write key with random name.
      final Map<String, CompletableFuture<Boolean>> map = writer.write(
          fileSize.getSize(), chunkSize.getSizeInt(), executor);
      for (String path : map.keySet()) {
        if (!map.get(path).join()) {
          Print.error(this, "Failed to write " + path);
        }
      }

      Print.elapsed(parameters.getSummary(), start);
    } finally {
      executor.shutdown();
    }
  }

  @Override
  public String toString() {
    return "Benchmark-" + id;
  }

  public static void main(String[] args) throws Exception {
    final Cli commandArgs = Cli.parse(args);

    final List<String> clients = Print.parseCommaSeparatedString(commandArgs.getClients());
    final Sync.Server launchSync = new Sync.Server(clients, commandArgs.getPort());
    new Thread(launchSync).start();

    final Benchmark benchmark = new Benchmark(commandArgs);
    int exit = 0;
    try {
      benchmark.run(launchSync);
    } catch (Throwable e) {
      Print.error("Benchmark", "Failed", e);
      exit = 1;
    } finally {
      System.exit(exit);
    }
  }
}
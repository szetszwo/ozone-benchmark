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
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Performance Benchmark
 */
public class Benchmark {
  enum Op {PREPARE_LOCAL_FILES, INIT_WRITER}

  interface Parameters {
    String getType();

    String getOm();

    int getFileNum();

    String getFileSize();

    String getChunkSize();

    String getLocalDirs();
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

  static OzoneClient getOzoneClient(String omAddress) throws IOException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.address", omAddress);
    conf.set("ozone.client.checksum.type", "NONE");
    return OzoneClientFactory.getRpcClient(conf);
  }

  private final String id = String.format("%08x", ThreadLocalRandom.current().nextInt());
  private final ExecutorService executor = Executors.newFixedThreadPool(1000);
  private final Parameters parameters;
  private final SizeInBytes fileSize;
  private final SizeInBytes chunkSize;

  Benchmark(Parameters parameters) {
    this.parameters = parameters;
    this.fileSize = SizeInBytes.valueOf(parameters.getFileSize());
    this.chunkSize = SizeInBytes.valueOf(parameters.getChunkSize());
  }

  List<String> prepareLocalFiles() throws Exception {
    Print.ln(Op.PREPARE_LOCAL_FILES, this);

    final List<File> localDirs = Utils.parseFiles(parameters.getLocalDirs()).stream()
        .map(dir -> new File(dir, id))
        .collect(Collectors.toList());
    Utils.createDirs(localDirs);

    final List<String> paths = Utils.generateLocalFiles(localDirs, parameters.getFileNum(), fileSize, executor);
    Utils.dropCache(fileSize, parameters.getFileNum(), localDirs.size());
    return paths;
  }

  Writer initWriter(List<String> paths, OzoneClient ozoneClient) throws IOException {
    Print.ln(Op.INIT_WRITER, "OzoneClient " + ozoneClient);
    // An Ozone ObjectStore instance is the entry point to access Ozone.
    final ObjectStore store = ozoneClient.getObjectStore();
    Print.ln(Op.INIT_WRITER, "Store " + store.getCanonicalServiceName());

    // Create volume with random name.
    final String volumeName = "bench-vol-" + id;
    store.createVolume(volumeName);
    final OzoneVolume volume = store.getVolume(volumeName);
    Print.ln(Op.INIT_WRITER, "Volume " + volumeName + " created.");

    // Create bucket with random name.
    final String bucketName = "bench-buck-" + id;
    volume.createBucket(bucketName);
    final OzoneBucket bucket = volume.getBucket(bucketName);
    Print.ln(Op.INIT_WRITER, "Bucket " + bucketName + " created.");

    final ReplicationConfig replication = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

    final Type type = Type.parse(parameters.getType());
    Print.ln(Op.INIT_WRITER, "Type " + type);
    final Writer writer = type.newWrite(paths).init(fileSize.getSize(), replication, bucket);
    Print.ln(Op.INIT_WRITER, writer);
    return writer;
  }

  void run(Sync.Server launchSync) throws Exception {
    final List<String> paths = prepareLocalFiles();
    // Get an Ozone RPC Client.
    try(OzoneClient ozoneClient = getOzoneClient(parameters.getOm())) {
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

      final Duration elapsed = Duration.between(start, Instant.now());
      Print.ln(writer + "_ELAPSED", elapsed);
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
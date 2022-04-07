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
  interface Parameters {
    String getType();

    String getOm();

    int getFileNum();

    int getFileSize();

    int getChunkSize();

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
      for(Type t : values()) {
        if (t.name().startsWith(s.trim().toUpperCase(Locale.ENGLISH))){
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

  static void benchmark(Parameters parameters, Sync.Server launchSync) throws Exception {
    final String id = String.format("%08x", ThreadLocalRandom.current().nextInt());
    Print.ln("BENCHMARK_ID", id);

    final List<File> localDirs = Utils.parseFiles(parameters.getLocalDirs()).stream()
        .map(dir -> new File(dir, id))
        .collect(Collectors.toList());
    Utils.createDirs(localDirs);

    final ExecutorService executor = Executors.newFixedThreadPool(1000);

    final int fileSize = parameters.getFileSize();
    final List<String> paths = Utils.generateLocalFiles(localDirs, parameters.getFileNum(), fileSize, executor);
    Utils.dropCache(fileSize, parameters.getFileNum(), localDirs.size());

    // Get an Ozone RPC Client.
    try(OzoneClient ozoneClient = getOzoneClient(parameters.getOm())) {
      Print.ln("Ozone", "client " + ozoneClient);
      // An Ozone ObjectStore instance is the entry point to access Ozone.
      final ObjectStore store = ozoneClient.getObjectStore();
      Print.ln("Ozone", "Store " + store.getCanonicalServiceName());

      // Create volume with random name.
      final String volumeName = "bench-vol-" + id;
      store.createVolume(volumeName);
      final OzoneVolume volume = store.getVolume(volumeName);
      Print.ln("Ozone", "Volume " + volumeName + " created.");

      // Create bucket with random name.
      final String bucketName = "bench-buck-" + id;
      volume.createBucket(bucketName);
      final OzoneBucket bucket = volume.getBucket(bucketName);
      Print.ln("Ozone", "Bucket " + bucketName + " created.");

      final ReplicationConfig replication = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

      final Type type = Type.parse(parameters.getType());
      Print.ln("Type", type);
      final Writer writer = type.newWrite(paths).init(fileSize, replication, bucket);
      Print.ln("Init", writer);

      // wait for sync signal
      launchSync.readyAndWait(true);

      final Instant start = Instant.now();
      // Write key with random name.
      final Map<String, CompletableFuture<Boolean>> map = writer.write(fileSize, parameters.getChunkSize(), executor);
      for (String path : map.keySet()) {
        if (!map.get(path).join()) {
          Print.error("Benchmark", "Failed to write " + path);
        }
      }

      final Duration elapsed = Duration.between(start, Instant.now());
      Print.ln(writer + "_ELAPSED", elapsed);
    }
  }

  public static void main(String[] args) throws Exception {
    final Cli commandArgs = Cli.parse(args);

    final List<String> clients = Print.parseCommaSeparatedString(commandArgs.getClients());
    final Sync.Server launchSync = new Sync.Server(clients, commandArgs.getPort());
    new Thread(launchSync).start();

    int exit = 0;
    try {
      benchmark(commandArgs, launchSync);
    } catch (Throwable e) {
      Print.error("Benchmark", "Failed", e);
      exit = 1;
    } finally {
      System.exit(exit);
    }
  }
}

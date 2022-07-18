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
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

abstract class OzoneWriter extends Writer {
  static final ReplicationConfig REPLICATION_CONFIG = ReplicationConfig.fromTypeAndFactor(
      ReplicationType.RATIS, ReplicationFactor.THREE);

  static OzoneClient getOzoneClient(String omAddress, SizeInBytes chunkSize) throws IOException {
    final BenchmarkConf conf = new BenchmarkConf();
    conf.set("ozone.om.address", omAddress);
    conf.set("ozone.client.datastream.min.packet.size", chunkSize);
    conf.printEntries();
    return OzoneClientFactory.getRpcClient(conf.getOzoneConfiguration());
  }

  static String deleteKeyIfExists(int i, OzoneBucket bucket) throws IOException {
    final String key = Benchmark.toItemName(i);
    OzoneKeyDetails exists = null;
    try {
      exists = bucket.getKey(key);
    } catch (OMException ignored) {
    }
    if (exists != null) {
      Print.ln(Benchmark.Op.INIT_WRITER, "Key " + key + " already exists; deleting it...");
      bucket.deleteKey(key);
    }
    return key;
  }

  private volatile OzoneBucket bucket;
  private volatile OzoneClient ozoneClient;

  OzoneWriter(List<File> localFiles) {
    super(localFiles);
  }

  OzoneBucket getBucket() {
    return bucket;
  }

  OzoneVolume initVolume(ObjectStore store, String id) throws IOException {
    final String volumeName = "bench-vol-" + id;
    try {
      final OzoneVolume exists = store.getVolume(volumeName);
      if (exists != null) {
        Print.ln(Benchmark.Op.INIT_WRITER, "Volume " + volumeName + " already exists.");
        return exists;
      }
    } catch (OMException ignored) {
    }
    store.createVolume(volumeName);
    Print.ln(Benchmark.Op.INIT_WRITER, "Volume " + volumeName + " created.");
    return store.getVolume(volumeName);
  }

  OzoneBucket initBucket(OzoneVolume volume, String id) throws IOException {
    final String bucketName = "bench-buck-" + id;
    try {
      final OzoneBucket exists = volume.getBucket(bucketName);
      if (exists != null) {
        Print.ln(Benchmark.Op.INIT_WRITER, "Bucket " + bucketName + " already exists.");
        return exists;
      }
    } catch (OMException ignored) {
    }
    volume.createBucket(bucketName);
    Print.ln(Benchmark.Op.INIT_WRITER, "Bucket " + bucketName + " created.");
    return volume.getBucket(bucketName);
  }

  @Override
  void init(Benchmark benchmark) throws IOException {
    ozoneClient = getOzoneClient(benchmark.getParameters().getServiceAddress(), benchmark.getChunkSize());
    Print.ln(Benchmark.Op.INIT_WRITER, "OzoneClient " + ozoneClient);
    // An Ozone ObjectStore instance is the entry point to access Ozone.
    final ObjectStore store = ozoneClient.getObjectStore();
    Print.ln(Benchmark.Op.INIT_WRITER, "Store " + store.getCanonicalServiceName());

    // Create volume.
    final OzoneVolume volume = initVolume(store, benchmark.getId());
    // Create bucket with random name.
    bucket = initBucket(volume, benchmark.getId());
    init(benchmark.getFileSize().getSize());
  }

  abstract void init(long fileSize) throws IOException;

  @Override
  BiFunction<Descriptor, ExecutorService, CompletableFuture<byte[]>> getRemoteMessageDigestFunction(Verifier verifier) {
    return (descriptor, executor) -> verifier.computeMessageDigestAsync(descriptor.getItemName(), bucket, executor);
  }

  @Override
  public void close() throws IOException {
    ozoneClient.close();
  }
}

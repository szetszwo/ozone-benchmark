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

import com.beust.jcommander.JCommander;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Performance Benchmark
 */
public class Benchmark {
  private static final int MB = 1 << 20;
  private static final int BUFFER_SIZE_IN_BYTES = 4*MB;

  private static final int NUM_DIRS = 16;

  static File getDir(File root, int n) {
    final int index = Math.toIntExact(Integer.toUnsignedLong(n) % NUM_DIRS);
    final String dir = String.format("dir%03d", index);
    return new File(root, dir);
  }

  static void createDirs(File root) throws IOException {
    for (int i = 0; i < NUM_DIRS; i++) {
      Files.createDirectory(getDir(root, i).toPath());
    }
  }

  static void runCommnad(String... args) {
    Objects.requireNonNull(args, "args == null");
    if (args.length == 0) {
      throw new IllegalArgumentException("args is empty.");
    }
    final int exitCode;
    try {
      exitCode = Runtime.getRuntime().exec(args).waitFor();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to execute " + Stream.of(args).reduce((a, b) -> a + " " + b), e);
    }
    if (exitCode != 0) {
      throw new IllegalStateException("Failed to execute " + Stream.of(args).reduce((a, b) -> a + " " + b)
          + ", exitCode=" + exitCode);
    }
  }

  static void sleepMs(long ms) throws InterruptedException {
    for(; ms > 0; ) {
      Print.ln("Sleep " + ms + "ms", "wait for disk write");
      final long sleep = Math.min(5000, ms);
      TimeUnit.MILLISECONDS.sleep(sleep);
      ms -= sleep;
    }
  }

  static void dropCache(int fileSize, int numFiles, int numDisks) throws InterruptedException {
    runCommnad("/bin/sh","-c","sync; echo 3 > /proc/sys/vm/drop_caches");

    final long safeTime = 5 * 1000L; // sleep extra 5 seconds.
    final long filesPerDisk = (numFiles - 1) / numDisks + 1;
    final long diskSpeed = 100_1000_1000L / 1000; // 100 MB / 1000ms
    final long msPerFile = (fileSize - 1) / diskSpeed + 1;
    sleepMs(filesPerDisk * msPerFile + safeTime);
  }

  static long writeLocalFileFast(String path, long sizeInBytes) {
    final long sizeInMB = (sizeInBytes >> 20) + 1;
    final String[] cmd = {"/bin/bash", "-c",
        "dd if=<(openssl enc -aes-256-ctr -pass pass:\"$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64)\" -nosalt < /dev/zero) of="
            + path + " bs=1M count=" + sizeInMB + " iflag=fullblock"};
    runCommnad(cmd);

    runCommnad("/usr/bin/truncate", "--size=" + sizeInBytes, path);
    return sizeInBytes;
  }

  static CompletableFuture<Long> writeLocalFileAsync(String path, int sizeInBytes, ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return writeLocalFileFast(path, sizeInBytes);
      } catch (Exception e) {
        throw new CompletionException("Failed to write " + path + ", size=" + sizeInBytes, e);
      }
    }, executor);
  }

  static String getPath(File root, String child) {
    return new File(getDir(root, child.hashCode()), child).getAbsolutePath();
  }

  static List<String> generateLocalFiles(File root, int numFiles, int fileSize, ExecutorService executor) {
    final UUID uuid = UUID.randomUUID();
    List<String> paths = new ArrayList<>();
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String path = getPath(root, "file-" + uuid + "-" + i);
      paths.add(path);
      futures.add(writeLocalFileAsync(path, fileSize, executor));
    }

    for (int i = 0; i < futures.size(); i ++) {
      final long size = futures.get(i).join();
      if (size != fileSize) {
        Print.error("generateLocalFiles", "path:" + paths.get(i) + " write:" + size
            + " mismatch expected size:" + fileSize);
      }
    }

    return paths;
  }

  static long writeLocalFile(String path, long fileSize, long bufferSize, int random) throws IOException {
    RandomAccessFile raf = null;
    long offset = 0;
    try {
      raf = new RandomAccessFile(path, "rw");
      while (offset < fileSize) {
        final long remaining = fileSize - offset;
        final long chunkSize = Math.min(remaining, bufferSize);
        byte[] buffer = new byte[(int)chunkSize];
        for (int i = 0; i < chunkSize; i ++) {
          buffer[i]= (byte) (i % random);
        }
        raf.write(buffer);
        offset += chunkSize;
      }
    } finally {
      if (raf != null) {
        raf.close();
      }
    }
    return offset;
  }

  static long writeByHeapByteBuffer(File file, OzoneOutputStream out) {
    final byte[] buffer = new byte[BUFFER_SIZE_IN_BYTES];
    int written = 0;
    try (FileInputStream in = new FileInputStream(file)) {
      for(;;) {
        final int read = in.read(buffer, 0, buffer.length);
        if (read == -1) {
          out.close();
          return written;
        }
        out.write(buffer, 0, read);
        written += read;
      }
    } catch (Throwable e) {
      throw new CompletionException("Failed to process " + file.getAbsolutePath(), e);
    }
  }

  static Map<String, CompletableFuture<Boolean>> writeByHeapByteBuffer(
      List<String> paths, List<OzoneOutputStream> outs, int fileSize, ExecutorService executor) {
    final Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();
    for(int i = 0; i < paths.size(); i ++) {
      final String path = paths.get(i);
      final OzoneOutputStream out = outs.get(i);
      final CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(
          () -> writeByHeapByteBuffer(new File(path), out) == fileSize, executor);
      fileMap.put(path, future);
    }
    return fileMap;
  }

  static long writeByFileRegion(File file, OzoneDataStreamOutput out) {
    try (FileInputStream fileInputStream = new FileInputStream(file)) {
      final FileChannel channel = fileInputStream.getChannel();
      final long size = channel.size();
      MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
      out.write(mappedByteBuffer);
      out.close();
      return size;
    } catch (Throwable e) {
      throw new CompletionException("Failed to process " + file.getAbsolutePath(), e);
    }
  }

  static Map<String, CompletableFuture<Boolean>> writeByFileRegion(
          List<String> paths, List<OzoneDataStreamOutput> outs, int fileSize, ExecutorService executor) {
    final Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();
    for(int i = 0; i < paths.size(); i ++) {
      final String path = paths.get(i);
      final OzoneDataStreamOutput out = outs.get(i);
      final CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(
          () -> writeByFileRegion(new File(path), out) == fileSize, executor);
      fileMap.put(path, future);
    }
    return fileMap;
  }

  static long writeByMappedByteBuffer(File file, OzoneDataStreamOutput out) {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r");) {
      final FileChannel channel = raf.getChannel();
      long off = 0;
      for (long len = raf.length(); len > 0; ) {
        final long writeLen = Math.min(len, BUFFER_SIZE_IN_BYTES);
        final ByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, off, writeLen);
        out.write(mapped);
        off += writeLen;
        len -= writeLen;
      }
      out.close();
      return off;
    } catch (Throwable e) {
      throw new CompletionException("Failed to process " + file.getAbsolutePath(), e);
    }
  }

  static Map<String, CompletableFuture<Boolean>> writeByMappedByteBuffer(
      List<String> paths, List<OzoneDataStreamOutput> outs, int fileSize, ExecutorService executor) {
    final Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();
    for(int i = 0; i < paths.size(); i ++) {
      String path = paths.get(i);
      final OzoneDataStreamOutput out = outs.get(i);
      final CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(
          () -> writeByMappedByteBuffer(new File(path), out) == fileSize, executor);
      fileMap.put(path, future);
    }
    return fileMap;
  }

  static OzoneClient getOzoneClient(String omAddress) throws IOException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.address", omAddress);
    conf.set("ozone.client.checksum.type", "NONE");
    return OzoneClientFactory.getRpcClient(conf);
  }

  public static void main(String[] args) throws Exception {
    final CommandArgs commandArgs = new CommandArgs();
    JCommander.newBuilder().addObject(commandArgs).build().parse(args);
    Print.ln("main", commandArgs);


    final Sync.Server launchSync = new Sync.Server(commandArgs.clients);
    new Thread(launchSync).start();

    int exit = 0;
    try {
      benchmark(commandArgs.om, commandArgs.volume, commandArgs.bucket,
          commandArgs.fileNum, commandArgs.fileSize, commandArgs.chunkSize, launchSync);
    } catch (Throwable e) {
      Print.error("Benchmark", "Failed", e);
      exit = 1;
    } finally {
      System.exit(exit);
    }
  }

  enum Type {ASYNC_API, STREAM_API};

  static void benchmark(String om, String volumeName, String bucketName,
      int numFiles, int fileSize, int chunkSize, Sync.Server launchSync) throws Exception {
    final File root = new File(".");
    createDirs(root);

    final ExecutorService executor = Executors.newFixedThreadPool(1000);
    final Type type = chunkSize == 0? Type.ASYNC_API: Type.STREAM_API;
    Print.ln("Type", "=== using " + type);

    dropCache(fileSize, numFiles, NUM_DIRS);
    final List<String> paths = generateLocalFiles(root, numFiles, fileSize, executor);

    // Get an Ozone RPC Client.
    try(OzoneClient ozoneClient = getOzoneClient(om)) {
      // An Ozone ObjectStore instance is the entry point to access Ozone.
      final ObjectStore store = ozoneClient.getObjectStore();

      // Create volume with random name.
      store.createVolume(volumeName);
      final OzoneVolume volume = store.getVolume(volumeName);
      Print.ln("Ozone", "Volume " + volumeName + " created.");

      // Create bucket with random name.
      volume.createBucket(bucketName);
      final OzoneBucket bucket = volume.getBucket(bucketName);
      Print.ln("Ozone", "Bucket " + bucketName + " created.");

      ReplicationConfig config =
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

      final List<OzoneDataStreamOutput> streamOuts = new ArrayList<>();
      final List<OzoneOutputStream> asyncOuts = new ArrayList<>();
      if (type == Type.STREAM_API) {
        for (int i = 0; i < paths.size(); i++) {
          OzoneDataStreamOutput out = bucket.createStreamKey(
              "ozonekey_" + i, fileSize, config, new HashMap<>());
          streamOuts.add(out);
        }
      } else {
        for (int i = 0; i < paths.size(); i++) {
          OzoneOutputStream out = bucket.createKey(
              "ozonekey_" + i, fileSize, config, new HashMap<>());
          asyncOuts.add(out);
        }
      }

      // wait for sync signal
      new Sync.Client(launchSync.getHosts()).sendReady();
      launchSync.waitAllReady();

      final Instant start = Instant.now();
      // Write key with random name.
      final Map<String, CompletableFuture<Boolean>> map;
      if (type == Type.STREAM_API) {
        map = writeByMappedByteBuffer(paths, streamOuts, fileSize, executor);
      } else {
        map = writeByHeapByteBuffer(paths, asyncOuts, fileSize, executor);
      }

      for (String path : map.keySet()) {
        CompletableFuture<Boolean> future = map.get(path);
        if (!future.join().booleanValue()) {
          Print.error("Benchmark", "Failed to write " + path);
        }
      }

      final Duration elapsed = Duration.between(start, Instant.now());
      Print.ln(type + "_ELAPSED", elapsed);
    }
  }
}

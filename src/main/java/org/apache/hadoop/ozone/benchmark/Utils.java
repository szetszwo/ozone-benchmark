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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

interface Utils {
  static void createDirs(List<File> localDirs) {
    for (int i = 0; i < localDirs.size(); i++) {
      final Path dir = localDirs.get(i).toPath();
      try {
        Files.deleteIfExists(dir);
        Files.createDirectories(dir);
      } catch (IOException e) {
        if (Files.isDirectory(dir)) {
          Print.error("createDirs", "Directory " + dir + " exists", e);
        } else {
          throw new IllegalStateException("Failed to process " + dir, e);
        }
      }
    }
  }

  static File getDir(int hash, List<File> localDirs) {
    final int i = Math.toIntExact(Integer.toUnsignedLong(hash) % localDirs.size());
    return localDirs.get(i);
  }

  static String getPath(String child, List<File> localDirs) {
    return new File(getDir(child.hashCode(), localDirs), child).getAbsolutePath();
  }

  static List<File> parse(String commaSeparated) {
    final List<File> files = new ArrayList<>();
    for(StringTokenizer t = new StringTokenizer(commaSeparated, ","); t.hasMoreTokens(); ) {
      files.add(new File(t.nextToken().trim()));
    }
    return files;
  }

  static void runCommand(String... args) {
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
    final String[] dropCacheCmd = {"/bin/sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"};
    try {
      runCommand(dropCacheCmd);
    } catch (Throwable e) {
      Print.error("dropCache", "Failed to runCommand " + Arrays.toString(dropCacheCmd));
    }

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
    runCommand(cmd);

    runCommand("/usr/bin/truncate", "--size=" + sizeInBytes, path);
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

  static List<String> generateLocalFiles(List<File> localDirs, int numFiles, int fileSize, ExecutorService executor) {
    final UUID uuid = UUID.randomUUID();
    final List<String> paths = new ArrayList<>();
    final List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      final String fileName = "file-" + uuid + "-" + i;
      final String path = getPath(fileName, localDirs);
      paths.add(path);
      futures.add(writeLocalFileAsync(path, fileSize, executor));
    }

    for (int i = 0; i < futures.size(); i ++) {
      final long size = futures.get(i).join();
      if (size != fileSize) {
        Print.error("generateLocalFiles", " Size mismatched " + paths.get(i)
            + ": writtenSize=" + size + " but expectedSize=" + fileSize);
      } else {
        Print.ln("generateLocalFiles", "Successfully written " + paths.get(i) + " with size=" + size);
      }
    }

    return paths;
  }

  static long writeLocalFile(String path, long fileSize, int bufferSize, int random) throws IOException {
    final byte[] buffer = new byte[bufferSize];
    long offset = 0;
    try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
      for(; offset < fileSize; ) {
        final long remaining = fileSize - offset;
        final int chunkSize = Math.toIntExact(Math.min(remaining, bufferSize));
        for (int i = 0; i < chunkSize; i ++) {
          buffer[i] = (byte) (i % random);
        }
        raf.write(buffer, 0, chunkSize);
        offset += chunkSize;
      }
    }
    return offset;
  }
}

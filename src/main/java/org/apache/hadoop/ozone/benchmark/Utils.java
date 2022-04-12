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

import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface Utils {
  enum Op {CREATE_DIRS, DROP_CACHE, LOCAL_FILES}

  static <U, V> Iterable<V> transform(Iterable<U> items, Function<U, V> function) {
    final Iterator<U> i = items.iterator();

    return () -> new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public V next() {
        return function.apply(i.next());
      }
    };
  }

  static void createDirs(Iterable<Path> localDirs) {
    for (Path dir : localDirs) {
      try {
        Files.deleteIfExists(dir);
        Files.createDirectories(dir);
      } catch (IOException e) {
        if (Files.isDirectory(dir)) {
          Print.error(Op.CREATE_DIRS, "Directory " + dir + " exists", e);
        } else {
          throw new IllegalStateException("Failed to process " + dir, e);
        }
      }
    }
  }

  static LocalDir getDir(int hash, List<LocalDir> localDirs) {
    final int i = Math.toIntExact(Integer.toUnsignedLong(hash) % localDirs.size());
    return localDirs.get(i);
  }

  static List<File> parseFiles(String commaSeparated) {
    return Print.parseCommaSeparatedString(commaSeparated).stream()
        .map(File::new)
        .collect(Collectors.toList());
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

  static void sleepMs(long ms, Object reason) throws InterruptedException {
    Print.ln("SLEEP " + ms + "ms", reason);
    for(int i = 0; ms > 0; i++) {
      final long sleep = Math.min(5000, ms);
      TimeUnit.MILLISECONDS.sleep(sleep);
      ms -= sleep;
      Print.ln("  sleep " + i, "remaining " + ms + "ms");
    }
  }

  static void dropCache(SizeInBytes fileSize, int numFiles, int numDisks) throws InterruptedException {
    final String[] dropCacheCmd = {"/bin/sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"};
    try {
      runCommand(dropCacheCmd);
    } catch (Throwable e) {
      Print.error(Op.DROP_CACHE, "Failed to runCommand " + Arrays.toString(dropCacheCmd));
    }

    final long safeTime = 5 * 1000L; // sleep extra 5 seconds.
    final long filesPerDisk = (numFiles - 1) / numDisks + 1;
    final long diskSpeed = 100_1000_1000L / 1000; // 100 MB / 1000ms
    final long msPerFile = (fileSize.getSize() - 1) / diskSpeed + 1;
    sleepMs(filesPerDisk * msPerFile + safeTime, Op.DROP_CACHE);
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

  static CompletableFuture<Long> writeLocalFileAsync(String path, SizeInBytes sizeInBytes, ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return writeLocalFileFast(path, sizeInBytes.getSize());
      } catch (Exception e) {
        throw new CompletionException("Failed to write " + path + ", size=" + sizeInBytes, e);
      }
    }, executor);
  }

  static List<String> generateLocalFiles(List<LocalDir> localDirs, int numFiles, SizeInBytes fileSize) {
    final UUID uuid = UUID.randomUUID();
    final List<String> paths = new ArrayList<>();
    final List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      final String fileName = "file-" + uuid + "-" + i;
      final LocalDir dir = getDir(fileName.hashCode(), localDirs);
      final String path = dir.getChild(fileName).getAbsolutePath();
      paths.add(path);
      futures.add(writeLocalFileAsync(path, fileSize, dir.getExecutor()));
    }

    for (int i = 0; i < futures.size(); i ++) {
      final long size = futures.get(i).join();
      if (size != fileSize.getSize()) {
        Print.error(Op.LOCAL_FILES, " Size mismatched " + paths.get(i)
            + ": writtenSize=" + size + " but expectedSize=" + fileSize);
      } else {
        Print.ln(Op.LOCAL_FILES, "Successfully written " + paths.get(i) + " with size=" + size);
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

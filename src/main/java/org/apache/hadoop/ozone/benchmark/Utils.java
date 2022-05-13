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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface Utils {
  enum Op {CREATE_DIRS, COMMAND, DROP_CACHE, LOCAL_FILES}

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
      if (Files.isDirectory(dir)) {
        Print.ln(Op.CREATE_DIRS, "Directory " + dir + " exists.");
      } else {
        try {
          Files.createDirectories(dir);
        } catch (IOException e) {
          throw new IllegalStateException("Failed to createDirectories " + dir, e);
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
    Print.ln(Op.COMMAND, Arrays.toString(args));
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

  static void checkSize(File file, SizeInBytes expectedSize) {
    final long writtenSize;
    try {
      writtenSize = Files.size(file.toPath());
    } catch (Throwable e) {
      throw new IllegalStateException("Failed to get size:  file=" + file + " , expectedSize=" + expectedSize, e);
    }
    if (writtenSize != expectedSize.getSize()) {
      throw new IllegalStateException("Size mismatched: file=" + file
          + ", writtenSize = " + writtenSize + " != expectedSize = " + expectedSize);
    }
  }

  static void writeLocalFileFast(File file, SizeInBytes fileSize) {
    final String path = file.getAbsolutePath();
    final long sizeInBytes = fileSize.getSize();
    final long sizeInMB = (sizeInBytes >> 20) + 1;
    final String[] cmd = {"/bin/bash", "-c",
        "dd if=<(openssl enc -aes-256-ctr -pass pass:\"$(dd if=/dev/urandom bs=128 count=1 2>/dev/null | base64)\" -nosalt < /dev/zero) of="
            + path + " bs=1M count=" + sizeInMB + " iflag=fullblock"};
    runCommand(cmd);
    runCommand("/usr/bin/truncate", "--size=" + sizeInBytes, path);

    checkSize(file, fileSize);
  }

  static File writeLocalFile(String path, SizeInBytes fileSize, SizeInBytes chunkSize) {
    final File file = new File(path);
    if (file.exists()) {
      Print.ln(Op.LOCAL_FILES, "File already exists " + path + " with expected size=" + fileSize);
      return file;
    }
    final Exception cmdException;
    try {
      writeLocalFileFast(file, fileSize);
      return file;
    } catch (Exception e) {
      cmdException = e;
    }
    try {
      writeLocalFile(file, fileSize.getSize(), chunkSize.getSizeInt(), ThreadLocalRandom.current().nextInt());
    } catch (Exception e) {
      e.addSuppressed(cmdException);
      throw new CompletionException("Failed to write " + path + ", size=" + fileSize, e);
    }
    return file;
  }

  static CompletableFuture<Void> writeLocalFileAsync(String path, SizeInBytes fileSize, SizeInBytes chunkSize,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> {
      checkSize(writeLocalFile(path, fileSize, chunkSize), fileSize);
      return null;
    }, executor);
  }

  static List<File> generateLocalFiles(List<LocalDir> localDirs, int numFiles, SizeInBytes fileSize, SizeInBytes chunkSize) {
    final List<File> paths = new ArrayList<>();
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      final String fileName = String.format("file-%s-%04d", fileSize, i).replace(" ", "");
      final LocalDir dir = getDir(fileName.hashCode(), localDirs);
      final String path = dir.getChild(fileName).getAbsolutePath();
      paths.add(new File(path));
      futures.add(writeLocalFileAsync(path, fileSize, chunkSize, dir.getExecutor()));
    }

    futures.forEach(CompletableFuture::join);
    return paths;
  }

  static void writeLocalFile(File file, long fileSize, int bufferSize, int random) throws IOException {
    final byte[] buffer = new byte[bufferSize];
    long offset = 0;
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
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
  }
}

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
import org.apache.hadoop.ozone.client.OzoneBucket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

abstract class Writer {
  static class KeyDescriptor {
    private final int index;
    private final File localFile;
    private final CompletableFuture<Boolean> writeFuture;
    private final CompletableFuture<Boolean> verifyFuture = new CompletableFuture<>();

    KeyDescriptor(File localFile, int index, CompletableFuture<Boolean> writeFuture) {
      this.index = index;
      this.localFile = localFile;
      this.writeFuture = writeFuture;
    }

    String getKey() {
      return Benchmark.toKey(index);
    }

    File getLocalFile() {
      return localFile;
    }

    boolean joinWriteFuture() {
      return writeFuture.join();
    }

    boolean joinVerifyFuture() {
      return verifyFuture.join();
    }

    void completeVerifyFuture(boolean b) {
      Print.ln(Benchmark.Op.VERIFY,  "Is " + this + " correctly written? " + b);
      verifyFuture.complete(b);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "[" + getKey() + ", " + getLocalFile().getName() + "]";
    }
  }

  static final ReplicationConfig REPLICATION_CONFIG = ReplicationConfig.fromTypeAndFactor(
      ReplicationType.RATIS, ReplicationFactor.THREE);

  private final List<File> localFiles;

  Writer(List<File> localFiles) {
    this.localFiles = localFiles;
  }

  List<File> getLocalFiles() {
    return localFiles;
  }

  File getLocalFile(int i) {
    return localFiles.get(i);
  }

  abstract Writer init(long fileSize, OzoneBucket bucket) throws IOException;

  abstract List<KeyDescriptor> write(long fileSize, int chunkSize, ExecutorService executor);

  CompletableFuture<Boolean> writeAsync(String name, Supplier<Long> writeMethod, long fileSize, ExecutorService executor) {
    final Instant start = Instant.now();
    Print.ln(this, "Start writing to " + name);
    return CompletableFuture.supplyAsync(writeMethod, executor)
        .thenApply(verify(name, fileSize, start))
        .exceptionally(e -> {
          Print.error(this, "Failed to write " + name, e);
          return false;
        });
  }

  Function<Long, Boolean> verify(String name, long fileSize, Instant start) {
    return writeSize -> {
      if (writeSize == fileSize) {
        Print.elapsed(this + ": Completed to write " + name, start);
        return true;
      } else {
        Print.error(this, "Failed to write " + name + ": writeSize = " + writeSize + " != fileSize = " + fileSize);
        return false;
      }
    };
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  static long writeByByteArray(File file, OutputStream out, int bufferSize) {
    final byte[] buffer = new byte[bufferSize];
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
}

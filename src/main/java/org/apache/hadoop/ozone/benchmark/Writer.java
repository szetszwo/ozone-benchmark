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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

abstract class Writer implements Closeable {
  enum State {
    WRITING,
    WRITE_FAILED,
    WRITE_SUCCEEDED,
    VERIFY_FAILED,
    VERIFY_SUCCEEDED,
  }

  static class Descriptor {
    private final int index;
    private final File localFile;
    private final CompletableFuture<Boolean> writeFuture;
    private final CompletableFuture<Boolean> verifyFuture = new CompletableFuture<>();

    Descriptor(File localFile, int index, CompletableFuture<Boolean> writeFuture) {
      this.index = index;
      this.localFile = localFile;
      this.writeFuture = writeFuture;
    }

    int getIndex() {
      return index;
    }

    String getItemName() {
      return Benchmark.toItemName(index);
    }

    File getLocalFile() {
      return localFile;
    }

    State getState() {
      if (verifyFuture.isDone()) {
        return verifyFuture.isCompletedExceptionally() || verifyFuture.isCancelled()? State.VERIFY_FAILED
            : State.VERIFY_SUCCEEDED;
      }
      if (writeFuture.isDone()) {
        return writeFuture.isCompletedExceptionally() || writeFuture.isCancelled()? State.WRITE_FAILED
            : State.WRITE_SUCCEEDED;
      }
      return State.WRITING;
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
      return getClass().getSimpleName() + "[" + getItemName() + ", " + getLocalFile().getName() + "]";
    }
  }

  static class DescriptorCountRunnable implements Runnable {
    private final List<Descriptor> descriptors;

    DescriptorCountRunnable(List<Descriptor> descriptors) {
      this.descriptors = descriptors;
    }

    @Override
    public void run() {
      for(;;) {
        final EnumMap<State, AtomicInteger> counters = new EnumMap<>(State.class);
        for(State s : State.values()) {
          counters.put(s, new AtomicInteger());
        }
        for(Descriptor k : descriptors) {
          counters.get(k.getState()).incrementAndGet();
        }
        Print.ln("COUNTS", counters);

        if (counters.get(State.WRITING).get() == 0) {
          return;
        }
        try {
          TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

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

  abstract void init(Benchmark benchmark) throws IOException;

  List<Descriptor> write(long fileSize, int chunkSize, ExecutorService executor) {
    final List<Descriptor> descriptors = writeImpl(fileSize, chunkSize, executor);
    new Thread(new DescriptorCountRunnable(descriptors)).start();
    return descriptors;
  }

  abstract List<Descriptor> writeImpl(long fileSize, int chunkSize, ExecutorService executor);

  CompletableFuture<Boolean> writeAsync(String name, Supplier<Long> writeMethod, long fileSize, ExecutorService executor) {
    final Instant start = Instant.now();
    Print.ln(this, "Start writing to " + name);
    return CompletableFuture.supplyAsync(writeMethod, executor)
        .thenApply(checkSize(name, fileSize, start))
        .exceptionally(e -> {
          Print.error(this, "Failed to write " + name, e);
          return false;
        });
  }

  Function<Long, Boolean> checkSize(String name, long fileSize, Instant start) {
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

  abstract BiFunction<Descriptor, ExecutorService, CompletableFuture<byte[]>> getRemoteMessageDigestFunction(Verifier verifier);

  void verify(Benchmark.Parameters parameters, SizeInBytes chunkSize,
      List<Descriptor> descriptors, ExecutorService executor) {
    final Instant verifyStartTime = Instant.now();
    for (Descriptor descriptor : descriptors) {
      final Verifier verifier = new Verifier(chunkSize.getSizeInt(), parameters.getMessageDigestAlgorithm());
      verifier.verifyMessageDigest(descriptor, getRemoteMessageDigestFunction(verifier), executor);
    }
    int errorCount = 0;
    for (Descriptor descriptor : descriptors) {
      if (!descriptor.joinVerifyFuture()) {
        Print.error(this, "Failed to verify " + descriptor);
        errorCount++;
      }
    }
    if (errorCount > 0) {
      throw new IllegalStateException("Failed to verify " + errorCount + " descriptors.");
    } else {
      Print.ln(Benchmark.Op.VERIFY, "All " + descriptors.size() + " descriptors are verified.");
    }
    Print.elapsed(Benchmark.Op.VERIFY, verifyStartTime);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  static long writeByByteArray(File file, OutputStream out, int bufferSize) {
    final byte[] buffer = new byte[bufferSize];
    long written = 0;
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

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
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Performance Benchmark
 */
public class Benchmark {
  enum Op {PREPARE_LOCAL_FILES, INIT_WRITER, VERIFY}

  interface Parameters {
    String getType();

    String getId();

    String getServiceAddress();

    int getFileNum();

    String getFileSize();

    String getChunkSize();

    boolean isVerify();

    String getMessageDigestAlgorithm();

    String getLocalDirs();

    boolean isDropCache();

    int getThreadNum();

    default String getSummary() {
      return getFileNum() + "x" + getFileSize() + "_" + getType() + "_c" + getChunkSize() + "_t" + getThreadNum();
    }
  }

  enum Type {
    ASYNC_API(AsyncWriter::new),
    STREAM_API_MAPPED_BYTE_BUFFER(StreamWriter.WithMappedByteBuffer::new),
    STREAM_API_BYTE_ARRAY(StreamWriter.WithByteArray::new),
    HDFS(HdfsWriter::new);

    private final Function<List<File>, Writer> constructor;

    Type(Function<List<File>, Writer> constructor) {
      this.constructor = constructor;
    }

    Writer newWrite(List<File> paths) {
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

  String getId() {
    return id;
  }

  Parameters getParameters() {
    return parameters;
  }

  SizeInBytes getFileSize() {
    return fileSize;
  }

  SizeInBytes getChunkSize() {
    return chunkSize;
  }

  static List<File> prepareLocalFiles(String id, int fileNum, SizeInBytes fileSize, SizeInBytes chunkSize,
      String localDirsString, boolean isDropCache) throws Exception {
    Print.ln(Op.PREPARE_LOCAL_FILES, id + ": fileNum=" + fileNum + ", fileSize=" + fileSize);

    final List<LocalDir> localDirs = Utils.parseFiles(localDirsString).stream()
        .map(dir -> new File(dir, id))
        .map(LocalDir::new)
        .collect(Collectors.toList());
    try {
      Utils.createDirs(Utils.transform(localDirs, LocalDir::getPath));

      final List<File> paths = Utils.generateLocalFiles(localDirs, fileNum, fileSize, chunkSize);
      if (isDropCache) {
        Utils.dropCache(fileSize, fileNum, localDirs.size());
      }
      return paths;
    } finally {
      localDirs.forEach(LocalDir::close);
    }
  }

  static String toItemName(int i) {
    return String.format("item_%04d", i);
  }

  void run(Sync.Server launchSync) throws Exception {
    final List<File> localFiles = prepareLocalFiles(id, parameters.getFileNum(), fileSize, chunkSize,
        parameters.getLocalDirs(), parameters.isDropCache());
    final ExecutorService executor = Executors.newFixedThreadPool(parameters.getThreadNum());
    final Type type = Type.parse(parameters.getType());
    Print.ln(Op.INIT_WRITER, "Type " + type);
    try(Writer writer = type.newWrite(localFiles)) {
      writer.init(this);
      Print.ln(Op.INIT_WRITER, writer);

      // wait for sync signal
      launchSync.readyAndWait(true);

      final List<Writer.Descriptor> keys = writeKeys(writer, executor);

      if (parameters.isVerify()) {
        writer.verify(parameters, chunkSize, keys, executor);
      }
    } finally {
      executor.shutdown();
    }
  }

  private List<Writer.Descriptor> writeKeys(Writer writer, ExecutorService executor) {
    // Write keys
    final Instant writeStartTime = Instant.now();
    final List<Writer.Descriptor> keys = writer.write(
        fileSize.getSize(), chunkSize.getSizeInt(), executor);
    int errorCount = 0;
    for (Writer.Descriptor key : keys) {
      if (!key.joinWriteFuture()) {
        Print.error(this, "Failed to write " + key);
        errorCount++;
      }
    }
    if (errorCount > 0) {
      throw new IllegalStateException("Failed to write " + errorCount + " keys.");
    } else {
      Print.ln(writer, "All " + keys.size() + " keys are written.");
    }
    Print.elapsed(parameters.getSummary(), writeStartTime);
    return keys;
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
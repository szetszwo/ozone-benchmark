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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

/** Write using the AsyncApi */
public class HdfsWriter extends Writer {
  private final List<FSDataOutputStream> outs = new ArrayList<>();

  private final Configuration conf = new Configuration();
  private volatile FileSystem fs;
  private volatile org.apache.hadoop.fs.Path dir;

  HdfsWriter(List<File> localFiles) {
    super(localFiles);
  }

  Path getFile(int i) {
    return new Path(dir, Benchmark.toItemName(i));
  }

  private Path deleteFileIfExists(int i) throws IOException {
    final Path file = getFile(i);
    FileStatus exists = null;
    try {
      exists = fs.getFileStatus(file);
    } catch (IOException ignored) {
    }
    if (exists != null) {
      Print.ln(Benchmark.Op.INIT_WRITER, "File " + file + " already exists; deleting it...");
      fs.delete(file, false);
    }
    return file;
  }

  @Override
  void init(Benchmark benchmark) throws IOException {
    fs = FileSystem.get(conf);
    dir = new Path("/benchmark/" + benchmark.getId());
    for (int i = 0; i < getLocalFiles().size(); i++) {
      final Path file = deleteFileIfExists(i);
      outs.add(fs.create(file, (short)3));
    }
  }

  @Override
  public List<Descriptor> writeImpl(long fileSize, int chunkSize, ExecutorService executor) {
    final List<Descriptor> keys = new ArrayList<>(getLocalFiles().size());
    for(int i = 0; i < getLocalFiles().size(); i ++) {
      final File localFile = getLocalFile(i);
      final FSDataOutputStream out = outs.get(i);
      final CompletableFuture<Boolean> future = writeAsync(
          localFile.getName(), () -> writeByByteArray(localFile, out, chunkSize),  fileSize, executor);
      keys.add(new Descriptor(localFile, i, future));
    }
    return keys;
  }

  @Override
  BiFunction<Descriptor, ExecutorService, CompletableFuture<byte[]>> getRemoteMessageDigestFunction(Verifier verifier) {
    return (descriptor, executor) -> CompletableFuture.supplyAsync(() -> {
      final Path file = getFile(descriptor.getIndex());
      try {
        return verifier.computeMessageDigest(fs.open(file));
      } catch (IOException e) {
        throw new CompletionException("Failed to open " + file, e);
      }
    }, executor);
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }
}

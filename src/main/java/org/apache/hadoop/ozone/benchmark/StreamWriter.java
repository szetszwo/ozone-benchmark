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

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

/** Write using the StreamApi */
public class StreamWriter extends Writer {
  private final List<OzoneDataStreamOutput> outs = new ArrayList<>();

  StreamWriter(List<File> localFiles) {
    super(localFiles);
  }

  @Override
  public StreamWriter init(long fileSize, OzoneBucket bucket) throws IOException {
    for (int i = 0; i < getLocalFiles().size(); i++) {
      final String key = Benchmark.deleteKeyIfExists(i, bucket);
      outs.add(bucket.createStreamKey( key, fileSize, REPLICATION_CONFIG, new HashMap<>()));
    }
    return this;
  }

  static long writeByMappedByteBuffer(File file, OzoneDataStreamOutput out, int chunkSize) {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      final FileChannel channel = raf.getChannel();
      long off = 0;
      for (long len = raf.length(); len > 0; ) {
        final long writeLen = Math.min(len, chunkSize);
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

  static long writeByByteArray(File file, OzoneDataStreamOutput out, int chunkSize) {
    return writeByByteArray(file, new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        final byte[] bytes = {(byte) (b & 0xFF)};
        write(bytes);
      }

      @Override
      public void write(@NotNull byte[] bytes) throws IOException {
        out.write(ByteBuffer.wrap(bytes));
      }

      @Override
      public void write(@NotNull byte[] bytes, int off, int len) throws IOException {
        out.write(ByteBuffer.wrap(bytes, off, len));
      }

      @Override
      public void close() throws IOException {
        out.close();
      }
    }, chunkSize);
  }

  @Override
  public List<KeyDescriptor> write(long fileSize, int chunkSize, ExecutorService executor) {
    final List<KeyDescriptor> keys = new ArrayList<>(getLocalFiles().size());
    for(int i = 0; i < getLocalFiles().size(); i ++) {
      final File localFile = getLocalFile(i);
      final OzoneDataStreamOutput out = outs.get(i);
      final CompletableFuture<Boolean> future = writeAsync(
          localFile.getName(), () -> writeByByteArray(localFile, out, chunkSize), fileSize, executor);
      keys.add(new KeyDescriptor(localFile, i, future));
    }
    return keys;
  }
}
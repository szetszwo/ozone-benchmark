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
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

/** Write using the AsyncApi */
public class AsyncWriter extends Writer {
  private final List<OzoneOutputStream> outs = new ArrayList<>();

  AsyncWriter(List<String> paths) {
    super(paths);
  }

  @Override
  public AsyncWriter init(int fileSize, ReplicationConfig replication, OzoneBucket bucket) throws IOException {
    for (int i = 0; i < getPaths().size(); i++) {
      outs.add(bucket.createKey( "ozonekey_" + i, fileSize, replication, new HashMap<>()));
    }
    return this;
  }

  static long writeByHeapByteBuffer(File file, OzoneOutputStream out, int bufferSize) {
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

  @Override
  public Map<String, CompletableFuture<Boolean>> write(int fileSize, int chunkSize, ExecutorService executor) {
    final Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();
    for(int i = 0; i < getPaths().size(); i ++) {
      final String path = getPath(i);
      final OzoneOutputStream out = outs.get(i);
      final CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(
          () -> writeByHeapByteBuffer(new File(path), out, chunkSize) == fileSize, executor);
      fileMap.put(path, future);
    }
    return fileMap;
  }
}

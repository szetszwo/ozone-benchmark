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
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/** Write using the AsyncApi */
public class AsyncWriter extends Writer {
  private final List<OzoneOutputStream> outs = new ArrayList<>();

  AsyncWriter(List<File> localFiles) {
    super(localFiles);
  }

  @Override
  public AsyncWriter init(long fileSize, OzoneBucket bucket) throws IOException {
    for (int i = 0; i < getLocalFiles().size(); i++) {
      final String key = Benchmark.deleteKeyIfExists(i, bucket);
      outs.add(bucket.createKey( key, fileSize, REPLICATION_CONFIG, new HashMap<>()));
    }
    return this;
  }

  @Override
  public List<KeyDescriptor> write(long fileSize, int chunkSize, ExecutorService executor) {
    final List<KeyDescriptor> keys = new ArrayList<>(getLocalFiles().size());
    for(int i = 0; i < getLocalFiles().size(); i ++) {
      final File localFile = getLocalFile(i);
      final OzoneOutputStream out = outs.get(i);
      final CompletableFuture<Boolean> future = writeAsync(
          localFile.getName(), () -> writeByByteArray(localFile, out, chunkSize) == fileSize, executor);
      keys.add(new KeyDescriptor(localFile, i, future));
    }
    return keys;
  }
}

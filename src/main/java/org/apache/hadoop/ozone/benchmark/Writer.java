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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

abstract class Writer {
  private final List<String> paths;

  Writer(List<String> paths) {
    this.paths = paths;
  }

  List<String> getPaths() {
    return paths;
  }

  String getPath(int i) {
    return paths.get(i);
  }

  abstract Writer init(int fileSize, ReplicationConfig replication, OzoneBucket bucket) throws IOException;

  abstract Map<String, CompletableFuture<Boolean>> write(int fileSize, int chunkSize, ExecutorService executor);
}

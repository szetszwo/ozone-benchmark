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
import org.apache.hadoop.ozone.client.io.OzoneInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

/** Verifier a remote key with a local file. */
class Verifier {
  private final byte[] buffer;
  private final String messageDigestAlgorithm;

  Verifier(int bufferSize, String messageDigestAlgorithm) {
    this.buffer = new byte[bufferSize];
    this.messageDigestAlgorithm = messageDigestAlgorithm;
  }

  MessageDigest getMessageDigest() {
    try {
      return MessageDigest.getInstance(messageDigestAlgorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(messageDigestAlgorithm + " not found", e);
    }
  }

  void verifyMessageDigest(Writer.KeyDescriptor key, OzoneBucket bucket, ExecutorService executor) {
    Print.ln(Benchmark.Op.VERIFY, "Start verifying " + key);
    final CompletableFuture<byte[]> local = computeMessageDigestAsync(key.getLocalFile(), executor);
    final CompletableFuture<byte[]> remote = computeMessageDigestAsync(key.getKey(), bucket, executor);
    local.thenCombine(remote, Arrays::equals).thenAccept(key::completeVerifyFuture);
  }

  CompletableFuture<byte[]> computeMessageDigestAsync(String key, OzoneBucket bucket, ExecutorService executor) {
    final String name = "computeMessageDigest for remote key " + key;
    return CompletableFuture.supplyAsync(() -> {
      try {
        return computeMessageDigest(name, key, bucket);
      } catch (IOException e) {
        throw new CompletionException("Failed " + name, e);
      }
    }, executor);
  }

  byte[] computeMessageDigest(String name, String key, OzoneBucket bucket) throws IOException {
    Print.ln(Benchmark.Op.VERIFY, name);
    try (OzoneInputStream in = bucket.readKey(key)) {
      return computeMessageDigest(in, buffer, getMessageDigest());
    }
  }

  CompletableFuture<byte[]> computeMessageDigestAsync(File localFile, ExecutorService executor) {
    final String name = "computeMessageDigest for local file " + localFile;
    return CompletableFuture.supplyAsync(() -> {
      try {
        return computeMessageDigest(name, localFile);
      } catch (IOException e) {
        throw new CompletionException("Failed " + name, e);
      }
    }, executor);
  }

  byte[] computeMessageDigest(String name, File localFile) throws IOException {
    Print.ln(Benchmark.Op.VERIFY, name);
    try (FileInputStream in = new FileInputStream(localFile)) {
      return computeMessageDigest(in, buffer, getMessageDigest());
    }
  }

  static byte[] computeMessageDigest(InputStream in, byte[] buffer, MessageDigest algorithm) throws IOException {
    for (; ; ) {
      final int n = in.read(buffer);
      if (n == -1) {
        break;
      } else if (n > 0) {
        algorithm.update(buffer, 0, n);
      }
    }
    return algorithm.digest();
  }
}

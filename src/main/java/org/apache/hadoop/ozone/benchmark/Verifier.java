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
import java.util.concurrent.CompletionException;

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

  boolean verifyMessageDigest(Writer.KeyDescriptor key, OzoneBucket bucket) {
    Print.ln(Benchmark.Op.VERIFY, "Start verifying " + key);
    try {
      final byte[] local = computeMessageDigest(key.getLocalFile(), buffer, getMessageDigest());
      final byte[] remote = computeMessageDigest(key.getIndex(), bucket, buffer, getMessageDigest());
      return Arrays.equals(local, remote);
    } catch (Throwable e) {
      throw new CompletionException("Failed to verifyMessageDigest for " + key, e);
    }
  }

  static byte[] computeMessageDigest(int i, OzoneBucket bucket, byte[] buffer, MessageDigest algorithm) throws IOException {
    final String key = Benchmark.toKey(i);
    Print.ln(Benchmark.Op.VERIFY, "computeMessageDigest for remote key " + key);
    try (OzoneInputStream in = bucket.readKey(key)) {
      return computeMessageDigest(in, buffer, algorithm);
    }
  }

  static byte[] computeMessageDigest(File path, byte[] buffer, MessageDigest algorithm) throws IOException {
    Print.ln(Benchmark.Op.VERIFY, "computeMessageDigest for local file " + path);
    try (FileInputStream in = new FileInputStream(path)) {
      return computeMessageDigest(in, buffer, algorithm);
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

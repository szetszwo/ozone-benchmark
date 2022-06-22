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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class Print {
  static final Logger LOG = LoggerFactory.getLogger(Print.class);

  private static final Instant START = Instant.now();
  private static final AtomicInteger ERROR_COUNT = new AtomicInteger();

  static void printShutdownMessage() {
    Print.ln("SHUTDOWN", "**************************************");
    Print.elapsed("SHUTDOWN", START);

    final int errorCount = ERROR_COUNT.get();
    Print.ln("SHUTDOWN", "ERROR_COUNT = " + errorCount);
    if (errorCount > 0) {
      System.exit(1);
    }
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(Print::printShutdownMessage));
  }

  static void elapsed(Object name, Instant start) {
    final Duration elapsed = Duration.between(start, Instant.now());
    Print.ln(name + ": ELAPSED", elapsed);
  }

  static synchronized void ln(Object name, Object message) {
    LOG.info(format(name, message));
  }

  static synchronized void error(Object name, Object message) {
    LOG.error(format(name, message));
  }

  static synchronized void error(Object name, Object message, Throwable e) {
    ERROR_COUNT.getAndIncrement();
    error(name, message);
    e.printStackTrace();
  }

  static String format(Object name, Object message) {
    if (message instanceof Duration) {
      message = duration2String((Duration) message);
    }
    return String.format("%s: %s", name, message);
  }

  static String randomId() {
    return String.format("%08x", ThreadLocalRandom.current().nextInt());
  }

  static String duration2String(Duration duration) {
    final long millis = duration.toMillis();
    return millis < 1000? millis + "ms": millis + "ms (" + duration.toString().substring(2) + ")";
  }

  static List<String> parseCommaSeparatedString(String commaSeparated) {
    final List<String> strings = new ArrayList<>();
    for(StringTokenizer t = new StringTokenizer(commaSeparated, ",;"); t.hasMoreTokens(); ) {
      strings.add(t.nextToken().trim());
    }
    return strings;
  }
}
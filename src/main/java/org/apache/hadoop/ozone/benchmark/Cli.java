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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

interface Cli extends Benchmark.Parameters {
  String getClients();

  int getPort();

  class Args implements Cli {
    @Parameter(names = "-type", description = "STREAM|ASYNC")
    private String type = "STREAM";
    @Parameter(names = "-id", description = "Benchmark ID")
    private String id = "";

    @Parameter(names = "-clients", description = "Comma-separated list of benchmark client <host:port> addresses.")
    private String clients = "127.0.0.1";
    @Parameter(names = "-port", description = "Server port for sync.")
    private int port = Sync.DEFAULT_PORT;

    @Parameter(names = "-om", description = "Ozone Manager address")
    private String om = "";

    @Parameter(names = "-fileNum", description = "The number of files.")
    private int fileNum = 4;
    @Parameter(names = "-fileSize", description = "The size of each file.")
    private String fileSize = "10mb";
    @Parameter(names = "-chunkSize", description = "The size of a chunk.")
    private String chunkSize = "2mb";

    @Parameter(names = "-verify", description = "Verify keys?")
    private boolean verify = false;
    @Parameter(names = "-messageDigestAlgorithm", description = "MD5|SHA-256")
    private String messageDigestAlgorithm = "MD5";

    static final String LOCAL_DIR = "-localDirs";
    @Parameter(names = LOCAL_DIR)
    private String localDirs = "";
    @Parameter(names = "-dropCache")
    private boolean dropCache = false;
    @Parameter(names = "-threadNum")
    private int threadNum = 100;

    private final JCommander jCommander = JCommander.newBuilder().addObject(this).build();

    JCommander getJCommander() {
      return jCommander;
    }

    void assertArgs() {
      if (localDirs.isEmpty()) {
        throw new IllegalArgumentException(LOCAL_DIR + " is not set.");
      }
    }

    @Override
    public String getType() {
      return type;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getClients() {
      return clients;
    }
    @Override
    public int getPort() {
      return port;
    }

    @Override
    public String getOm() {
      return om;
    }
    @Override
    public int getFileNum() {
      return fileNum;
    }
    @Override
    public String getFileSize() {
      return fileSize;
    }
    @Override
    public String getChunkSize() {
      return chunkSize;
    }

    @Override
    public boolean isVerify() {
      return verify;
    }
    @Override
    public String getMessageDigestAlgorithm() {
      return messageDigestAlgorithm;
    }

    @Override
    public String getLocalDirs() {
      return localDirs;
    }

    @Override
    public boolean isDropCache() {
      return dropCache;
    }

    @Override
    public int getThreadNum() {
      return threadNum;
    }

    @Override
    public String getSummary() {
      final int n = Print.parseCommaSeparatedString(getClients()).size();
      final String clientString = n <= 1? "": n + "clients_";
      return clientString + Cli.super.getSummary();
    }

    @Override
    public String toString() {
      return clients + ", port=" + port + ", om=" + om
          + "\n          type = " + type
          + "\n       fileNum = " + fileNum
          + "\n      fileSize = " + fileSize
          + "\n     chunkSize = " + chunkSize
          + "\n     threadNum = " + threadNum
          + "\n        verify = " + (verify? messageDigestAlgorithm: "NO")
          + "\n     localDirs = " + localDirs;
    }
  }

  static Cli parse(String... args) {
    final Args cliArgs = new Args();
    cliArgs.getJCommander().parse(args);
    Print.ln("parse", cliArgs);
    cliArgs.assertArgs();
    return cliArgs;
  }

  static void usage() {
    new Args().getJCommander().usage();
  }

  static void main(String[] args) {
    usage();
  }
}
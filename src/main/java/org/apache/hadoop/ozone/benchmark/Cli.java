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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public interface Cli {
  int MB = 1 << 20;

  String getType();

  List<String> getClients();

  int getPort();

  String getOm();

  String getVolume();

  String getBucket();

  int getFileNum();

  int getFileSize();

  int getChunkSize();

  String getLocalDirs();

  class Args implements Cli {
    @Parameter(names = "-type", description = "STREAM|ASYNC")
    private String type = "STREAM";

    @Parameter(names = "-clients", description = "Comma-separated list of benchmark client <host:port> addresses.")
    private List<String> clients = new ArrayList<>(Collections.singletonList("127.0.0.1"));
    @Parameter(names = "-port", description = "Server port for sync.")
    private int port = Sync.DEFAULT_PORT;

    @Parameter(names = "-om", description = "Ozone Manager address")
    private String om = "127.0.0.1";
    @Parameter(names = "-volume", description = "Ozone Object Store volume name")
    private String volume = "testVolume";
    @Parameter(names = "-bucket", description = "Ozone Object Store bucket name")
    private String bucket = "testBucket";

    @Parameter(names = "-fileNum", description = "The number of files.")
    private int fileNum = 10;
    @Parameter(names = "-fileSize", description = "The size of each file.")
    private int fileSize = 100 * MB;
    @Parameter(names = "-chunkSize", description = "The size of a chunk.")
    private int chunkSize = 2 * MB;
    @Parameter(names = "-checksum", description = "Run with checksum enabled?")
    private boolean checksum = false;

    static final String LOCAL_DIR = "-localDirs";
    @Parameter(names = LOCAL_DIR)
    private String localDirs = "";

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
    public List<String> getClients() {
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
    public String getVolume() {
      return volume;
    }
    @Override
    public String getBucket() {
      return bucket;
    }
    @Override
    public int getFileNum() {
      return fileNum;
    }
    @Override
    public int getFileSize() {
      return fileSize;
    }
    @Override
    public int getChunkSize() {
      return chunkSize;
    }

    @Override
    public String getLocalDirs() {
      return localDirs;
    }

    @Override
    public String toString() {
      return clients + ", port=" + port
          + "\n          type = " + type
          + "\n            om = '" + om + '\''
          + "\n        volume = '" + volume + '\''
          + "\n        bucket = '" + bucket + '\''
          + "\n       fileNum = " + fileNum
          + "\n      fileSize = " + fileSize
          + "\n     chunkSize = " + chunkSize
          + "\n      checksum = " + checksum
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
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

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CommandArgs {
  static final int MB = 1 << 20;

  @Parameter(names = "-clients", description = "Comma-separated list of benchmark client ip")
  public List<String> clients = new ArrayList<>(Collections.singletonList("127.0.0.1"));

  @Parameter(names = "-om")
  public String om = "om";

  @Parameter(names = "-volume")
  public String volume = "testVolume";

  @Parameter(names = "-bucket")
  public String bucket = "testBucket";

  @Parameter(names = "-fileNum")
  public int fileNum = 10;

  @Parameter(names = "-fileSize")
  public int fileSize = 100 * MB;

  @Parameter(names = "-chunkSize")
  public int chunkSize = 1 * MB;

  @Parameter(names = "-checksum")
  public boolean checksum = false;

  @Parameter(names = "-localRootDir")
  public String localRootDir = "benchmarkData";

  @Override
  public String toString() {
    return clients
        + "\n        volume = '" + volume + '\''
        + "\n        bucket = '" + bucket + '\''
        + "\n       fileNum = " + fileNum
        + "\n     chunkSize = " + chunkSize
        + "\n      fileSize = " + fileSize
        + "\n      checksum = " + checksum
        + "\n  localRootDir = " + localRootDir
        ;
  }
}
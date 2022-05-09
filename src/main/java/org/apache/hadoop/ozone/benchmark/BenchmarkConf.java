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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class BenchmarkConf {
  static String[] KEYS = {
      "scm.container.client.max.size",

      "ozone.client.checksum.type",

      "ozone.client.datastream.buffer.flush.size",
      "ozone.client.datastream.window.size",
      "ozone.client.datastream.min.packet.size",
      "ozone.client.datastream.pipeline.mode",

      "hdds.ratis." + NettyConfigKeys.DataStream.Client.WORKER_GROUP_SHARE_KEY,
      "hdds.ratis." + NettyConfigKeys.DataStream.Client.WORKER_GROUP_SIZE_KEY,
  };

  static class Values {
    private final String defaultValue;
    private String newValue;

    Values(String defaultValue) {
      this.defaultValue = defaultValue;
    }

    boolean setValue(String value) {
      if (value != null && !value.equals(defaultValue)) {
        newValue = value;
        return true;
      }
      return false;
    }

    boolean setValue(SizeInBytes size) {
      if (size == null) {
        return false;
      }
      if (defaultValue != null) {
        final SizeInBytes defaultSize = SizeInBytes.valueOf(defaultValue);
        if (size.getSize() == defaultSize.getSize()) {
          return false;
        }
      }
      setValue(size.getSize() + "B");
      return true;
    }

    String getValue() {
      return newValue != null? newValue: defaultValue;
    }

    @Override
    public String toString() {
      return newValue != null? newValue + " (custom, default=" + defaultValue + ")"
          : defaultValue + " (default)";
    }
  }

  private final OzoneConfiguration conf = new OzoneConfiguration();
  private final SortedMap<String, Values> entries = new TreeMap<>();

  BenchmarkConf() {
    final File f = new File("benchmark-conf.xml");
    Print.ln(f.getAbsolutePath(), "exists? " + f.exists());

    for(String key : KEYS) {
      entries.put(key, new Values(conf.get(key)));
    }

    conf.addResource(new Path(f.toURI()));
    for(String key : KEYS) {
      entries.get(key).setValue(conf.get(key));
    }
  }

  OzoneConfiguration getOzoneConfiguration() {
    return conf;
  }

  void set(String key, String value) {
    if (entries.computeIfAbsent(key, k -> new Values(null)).setValue(value)) {
      conf.set(key, value);
    }
  }

  void set(String key, SizeInBytes size) {
    final Values values = entries.computeIfAbsent(key, k -> new Values(null));
    if (values.setValue(size)) {
      conf.set(key, values.getValue());
    }
  }

  void printEntries() {
    for(Map.Entry<String, Values> e : entries.entrySet()) {
      Print.ln(e.getKey(), e.getValue());
    }
  }

  public static void main(String[] args) throws Exception {
    new BenchmarkConf().printEntries();
  }
}
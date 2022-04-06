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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Sync {
  static final int PORT = 55666;
  private final List<String> hosts;

  Sync(List<String> hosts) {
    this.hosts = hosts;
  }

  List<String> getHosts() {
    return hosts;
  }

  static class Server extends Sync implements Runnable {
    private final ServerSocket serverSocket;
    private final CountDownLatch latch;

    public Server(List<String> clients) throws IOException {
      super(clients);
      this.serverSocket = new ServerSocket(PORT);
      serverSocket.setSoTimeout(1000);

      final int n = clients.size();
      this.latch = new CountDownLatch(n);
    }

    void waitAllReady() throws InterruptedException {
      latch.await();
      Print.ln(this, "ALL_READY");
    }

    boolean accept(Socket accepted) {
      final SocketAddress remote = accepted.getRemoteSocketAddress();
      Print.ln(this, "Accepted " + remote);
      try (Socket socket = accepted) {
        final DataInputStream in = new DataInputStream(socket.getInputStream());
        Print.ln(this, "Received from client " + remote + ": " + in.readUTF());
        final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF("ACK_" + accepted.getLocalSocketAddress());
        return true;
      } catch (Exception e) {
        Print.error(this, "Failed remote " + remote, e);
        return false;
      }
    }

    boolean accept(Executor executor) {
      for (final Instant start = Instant.now(); ; ) {
        final Duration elapsed = Duration.between(start, Instant.now());
        Print.ln(this, "Listening at " + serverSocket.getLocalSocketAddress()
            + " ... elapsed " + Print.duration2String(elapsed));
        try {
          final Socket accepted = serverSocket.accept();
          CompletableFuture.supplyAsync(() -> accept(accepted), executor).thenAccept(success -> {
            if (success) {
              latch.countDown();
            }
          });
        } catch (SocketTimeoutException e) {
          if (latch.getCount() == 0) {
            return true;
          }
        } catch (Throwable e) {
          Print.error(this, "Failed to accept", e);
          break;
        }
      }
      return false;
    }

    @Override
    public void run() {
      final ExecutorService executor = Executors.newFixedThreadPool(getHosts().size());
      try {
        accept(executor);
      } finally {
        executor.shutdown();
      }
    }

    @Override
    public String toString() {
      return "Sync.Server";
    }
  }

  static class Client extends Sync {
    Client(List<String> clients) {
      super(clients);
    }

    boolean sendReady(String server) {
      Print.ln(this, "Connecting to " + server + ":" + PORT);
      try (Socket client = new Socket(server, PORT)) {
        Print.ln(this, "Connected to " + server + ":" + PORT + " from " + client.getLocalSocketAddress());
        final DataOutputStream out = new DataOutputStream(client.getOutputStream());
        out.writeUTF("READY_" + client.getLocalSocketAddress());
        final DataInputStream in = new DataInputStream(client.getInputStream());
        Print.ln(this, "Received from server " + server + ": " + in.readUTF());
        return true;
      } catch (IOException e) {
        Print.error(this, "Failed to set ready to " + server + ":" + PORT, e);
        return false;
      }
    }

    void sendReady() {
      final ExecutorService executor = Executors.newFixedThreadPool(getHosts().size());
      try {
        for (String host : getHosts()) {
          CompletableFuture.supplyAsync(() -> sendReady(host), executor);
        }
      } finally {
        executor.shutdown();
      }
    }

    @Override
    public String toString() {
      return "Sync.Client";
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    final List<String> hosts = Collections.singletonList("127.0.0.1");
    final Sync.Server Sync = new Sync.Server(hosts);
    new Thread(Sync).start();
    new Sync.Client(hosts).sendReady();
    Sync.waitAllReady();
  }
}
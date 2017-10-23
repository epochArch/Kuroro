/*
 * Copyright 2017 EpochArch.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epocharch.kuroro.common.inner.monitor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(CloseMonitor.class);
  private final static String DEFAULT_SHUTDOWN_CMD = "shutdown";

  public void start(int port, boolean isExit, CloseHook hook, String prompt) {
    start(port, DEFAULT_SHUTDOWN_CMD, isExit, hook, prompt);
  }

  public void start(final int port, final String cmd, final boolean isExist, final CloseHook hook,
      final String prompt) {
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          ServerSocket ss = new ServerSocket(port);
          LOG.info("MonitorTask started at port: " + port);
          Socket socket = null;
          while (true) {
            try {
              socket = ss.accept();
              LOG.info("Accepted one connection : " + socket.getRemoteSocketAddress());
              BufferedReader br = new BufferedReader(
                  new InputStreamReader(socket.getInputStream()));
              String command = br.readLine();
              LOG.info("Command : " + command);
              if (cmd.equals(command)) {
                LOG.info("Shutdown command received.");
                break;
              }
            } catch (Exception e) {

            } finally {
              if (socket != null) {
                socket.close();
              }
            }
          }
          hook.onClose();
          if (isExist) {
            LOG.info("Server shutdown finished!!!");
            System.exit(0);
          }
        } catch (Exception e) {
          LOG.error("CloseMonitor start failed.exit(1)", e);
          if (isExist) {
            System.exit(1);
          }
        }
      }
    };
    t.setDaemon(true);
    t.setName("CloseMonitor");
    t.start();
  }

  public interface CloseHook {

    void onClose();
  }
}

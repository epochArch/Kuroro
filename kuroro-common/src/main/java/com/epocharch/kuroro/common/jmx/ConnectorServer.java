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

package com.epocharch.kuroro.common.jmx;

import java.io.IOException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorServer {

  private static final Logger logger = LoggerFactory.getLogger(ConnectorServer.class);
  private JMXConnectorServer connectorServer;
  private Registry rmiRegistry;
  private MBeanServer server;
  private boolean rmiCreated;
  private boolean threaded = true;
  private boolean daemon = true;
  private Set<ObjectName> registeredBeanNames = new LinkedHashSet<ObjectName>();
  private JMXServiceURL jmxServiceUrl;

  public void setJmxServiceUrl(JMXServiceURL jmxServiceUrl) {
    this.jmxServiceUrl = jmxServiceUrl;
  }

  public void setServer(MBeanServer server) {
    this.server = server;
  }

  public void setRmiRegistry(Registry rmiRegistry, boolean rmiCreated) {
    this.rmiRegistry = rmiRegistry;
    this.rmiCreated = rmiCreated;
  }

  public void setConnectorServer(JMXConnectorServer connectorServer) {
    this.connectorServer = connectorServer;
  }

  public void doRegister(ObjectName objectName, Object mbean) throws JMException {
    try {
      ObjectInstance registeredBean = this.server.registerMBean(mbean, objectName);
      this.registeredBeanNames.add(registeredBean.getObjectName());
    } catch (InstanceAlreadyExistsException e) {
      logger.info("MBean is register");
    }
  }

  private void unregisterBeans() {
    for (ObjectName registeredBeanName : registeredBeanNames) {
      doUnregister(registeredBeanName);
    }
    this.registeredBeanNames.clear();
  }

  public void start() {
    try {
      if (this.threaded) {
        Thread connectorThread = new Thread() {
          public void run() {
            try {
              ConnectorServer.this.connectorServer.start();
              logger
                  .info("JMX connector server[" + ConnectorServer.this.connectorServer.getAddress()
                      + "] started successfully.");
            } catch (IOException e) {
              logger.error(
                  "JMX connector server start asynchronizedly failed, maybe already started one.",
                  e);
            }
          }
        };
        connectorThread.setName("JMX connector thread [" + this.jmxServiceUrl + "]");
        connectorThread.setDaemon(this.daemon);
        connectorThread.start();
      } else {
        this.connectorServer.start();
        logger.info("JMX connector server[" + this.connectorServer.getAddress()
            + "] started successfully.");
      }
    } catch (IOException e) {
      logger.error("JMX connector server start failed, maybe already started one.", e);
      unregisterBeans();
    }
  }

  public void stop() throws Exception {
    logger.info("Stop JMX connector server.");
    try {
      this.connectorServer.stop();
    } finally {
      unregisterBeans();
      // stop rmi registry if newly created
      if (rmiCreated) {
        UnicastRemoteObject.unexportObject(rmiRegistry, true);
      }
    }
  }

  private void doUnregister(ObjectName objectName) {
    try {
      if (this.server.isRegistered(objectName)) {
        this.server.unregisterMBean(objectName);
      }
    } catch (JMException e) {
      logger.error("Could not unregister MBean with name[" + objectName + "].", e);
    }
  }

  public void setThreaded(boolean threaded) {
    this.threaded = threaded;
  }

  public void setDaemon(boolean daemon) {
    this.daemon = daemon;
  }
}

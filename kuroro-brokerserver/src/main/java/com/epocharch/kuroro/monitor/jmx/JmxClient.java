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

package com.epocharch.kuroro.monitor.jmx;

import com.epocharch.kuroro.common.jmx.support.JMXServiceURLBuilder;
import java.io.IOException;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;

public class JmxClient {

  private final String host;
  private final Integer port;
  private MBeanServerConnection mbeanServerConnection;
  private JMXConnector conn;

  public JmxClient(String host, Integer port) throws IOException {
    this.port = port;
    this.host = host;
    JMXServiceURLBuilder builder = new JMXServiceURLBuilder(host, port);
    conn = JMXConnectorFactory.connect(builder.getJMXServiceURL());
    mbeanServerConnection = conn.getMBeanServerConnection();
  }

  public void Connect() throws IOException {
    conn.connect();
  }

  public void Close() throws IOException {
    conn.close();
    this.mbeanServerConnection = null;
    this.conn = null;
  }

  public Object getAttribute(ObjectName objectName, String attribute)
      throws AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, IOException {
    Object obj = mbeanServerConnection.getAttribute(objectName, attribute);
    return obj;
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  public MBeanServerConnection getMbeanServerConnection() {
    return mbeanServerConnection;
  }
}

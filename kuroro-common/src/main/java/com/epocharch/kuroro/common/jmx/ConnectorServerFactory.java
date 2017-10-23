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

import com.epocharch.kuroro.common.jmx.support.JMXServiceURLBuilder;
import com.epocharch.kuroro.common.jmx.support.JmxUtils;
import com.thoughtworks.xstream.XStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

public class ConnectorServerFactory {

  private static ConnectorServer connectorServer = null;

  public static ConnectorServer createConnectorServer(Map<String, Object> environment,
      ObjectName objectName, String path, int port,
      MBeanServer server) throws Exception {
    if (connectorServer != null) {
      return connectorServer;
    }
    connectorServer = new ConnectorServer();
    // registry rmi
    JMXServiceURLBuilder jmxServiceURLBuilder = new JMXServiceURLBuilder(port, path);
    JMXServiceURL jmxServiceUrl = jmxServiceURLBuilder.getJMXServiceURL();
    if ("rmi".equals(jmxServiceUrl.getProtocol())) {
      Registry rmiRegistry = null;
      try {
        rmiRegistry = LocateRegistry.getRegistry(port);
        connectorServer.setRmiRegistry(rmiRegistry, false);
      } catch (RemoteException e) {
        rmiRegistry = LocateRegistry.createRegistry(port);
        connectorServer.setRmiRegistry(rmiRegistry, true);
      }
    }
    connectorServer.setJmxServiceUrl(jmxServiceUrl);
    // connector server
    if (server == null) {
      server = JmxUtils.locateMBeanServer();
    }

    connectorServer.setServer(server);
    JMXConnectorServer jmxConnectorServer = JMXConnectorServerFactory
        .newJMXConnectorServer(jmxServiceUrl, environment,
            (MBeanServer) Proxy.newProxyInstance(server.getClass().getClassLoader(),
                server.getClass().getInterfaces(),
                new ValueParseHandler(server)));
    connectorServer.setConnectorServer(jmxConnectorServer);
    if (objectName != null) {
      connectorServer.doRegister(objectName, jmxConnectorServer);
    }
    return connectorServer;
  }

  private static class ValueParseHandler implements InvocationHandler {

    private static Collection<String> methodsList = Arrays.asList("invoke", "getAttribute");
    private final MBeanServer server;
    private XStream xstream = new XStream();

    public ValueParseHandler(MBeanServer server) {
      this.server = server;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Object retVal = method.invoke(this.server, args);
      if (methodsList.contains(method.getName())) {
        return reprocess(retVal);
      }
      if ("getAttributes".equals(method.getName())) {
        AttributeList attrList = (AttributeList) retVal;
        if (attrList == null) {
          return attrList;
        }
        AttributeList reprocessedList = new AttributeList(attrList.size());
        for (Object attr : attrList) {
          Attribute attribute = (Attribute) attr;
          Object attrValue = attribute.getValue();
          reprocessedList.add(new Attribute(attribute.getName(), reprocess(attrValue)));
        }
        return reprocessedList;
      }
      return retVal;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object reprocess(Object retVal) {
      if (retVal != null) {
        Class<?> clazz = retVal.getClass();
        if (Set.class.isAssignableFrom(clazz) && isConcurrentOrInnerClass(clazz)) {
          retVal = new HashSet((Set<?>) retVal);
        } else if (List.class.isAssignableFrom(clazz) && isConcurrentOrInnerClass(clazz)) {
          retVal = new ArrayList((List<?>) retVal);
        } else if (Map.class.isAssignableFrom(clazz) && isConcurrentOrInnerClass(clazz)) {
          retVal = new HashMap((Map<?, ?>) retVal);
        }
      }
      try {
        return xstream.toXML(retVal);
      } catch (Exception e) {
        String errorMsg = "Xstream marshall retVal[" + retVal + "] failed.";
        throw new RuntimeException(errorMsg);
      }
    }

    private boolean isConcurrentOrInnerClass(Class<?> clazz) {
      return clazz.getSimpleName().toLowerCase().contains("concurrent") || clazz.getName()
          .contains("$");
    }
  }

}

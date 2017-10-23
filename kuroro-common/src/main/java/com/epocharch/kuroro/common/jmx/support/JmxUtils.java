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

package com.epocharch.kuroro.common.jmx.support;

import java.util.List;
import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.MBeanServerNotFoundException;

public class JmxUtils {

  private static final Logger logger = LoggerFactory.getLogger(JmxUtils.class);
  private static final String MBEAN_SUFFIX = "MBean";

  private static final String MXBEAN_SUFFIX = "MXBean";

  public static MBeanServer locateMBeanServer() throws MBeanServerNotFoundException {
    List<MBeanServer> servers = MBeanServerFactory.findMBeanServer(null);

    MBeanServer server = null;
    if (servers != null && servers.size() > 0) {
      if (servers.size() > 1) {
        logger.warn("More than one mbean server found, use first one.");
      }
      server = (MBeanServer) servers.get(0);
    }

    if (server == null) {
      throw new MBeanServerNotFoundException("Unable to locate an mbean server");
    }

    return server;
  }

  public static boolean isMBean(Class<? extends Object> beanClass) {
    return (beanClass != null && (DynamicMBean.class.isAssignableFrom(beanClass)
        || getMBeanInterface(beanClass) != null
        || getMXBeanInterface(beanClass) != null));
  }

  public static Class<?> getMBeanInterface(Class<?> clazz) {
    if (clazz.getSuperclass() == null) {
      return null;
    }
    String mbeanInterfaceName = clazz.getName() + MBEAN_SUFFIX;
    Class<?>[] implementedInterfaces = clazz.getInterfaces();
    for (int x = 0; x < implementedInterfaces.length; x++) {
      Class<?> iface = implementedInterfaces[x];
      if (iface.getName().equals(mbeanInterfaceName)) {
        return iface;
      }
    }
    return getMBeanInterface(clazz.getSuperclass());
  }

  public static Class<?> getMXBeanInterface(Class<?> clazz) {
    if (clazz.getSuperclass() == null) {
      return null;
    }
    Class<?>[] implementedInterfaces = clazz.getInterfaces();
    for (int x = 0; x < implementedInterfaces.length; x++) {
      Class<?> iface = implementedInterfaces[x];
      boolean isMxBean = iface.getName().endsWith(MXBEAN_SUFFIX);
      if (isMxBean) {
        return iface;
      }
    }
    return getMXBeanInterface(clazz.getSuperclass());
  }
}

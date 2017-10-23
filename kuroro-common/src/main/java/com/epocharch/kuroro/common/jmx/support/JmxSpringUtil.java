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

import com.epocharch.kuroro.common.constants.InternalPropKey;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.MBeanExporter;
import org.springframework.jmx.support.ObjectNameManager;

public class JmxSpringUtil {

  public static final String JMX_DOMAIN = InternalPropKey.JMX_DOMAIN_NAME;
  private static Logger logger = LoggerFactory.getLogger(JmxSpringUtil.class);
  private MBeanExporter mbeanExporter;

  public void setMbeanExporter(MBeanExporter mbeanExporter) {
    this.mbeanExporter = mbeanExporter;
  }

  public void registerMBean(String name, Object bean) {
    try {
      ObjectName objectName = ObjectNameManager.getInstance(createObjectName(name));
      mbeanExporter.unregisterManagedResource(objectName);
      mbeanExporter.registerManagedResource(bean, objectName);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  public void unregisterMBean(String name) {
    try {
      ObjectName objectName = ObjectNameManager.getInstance(createObjectName(name));
      mbeanExporter.unregisterManagedResource(objectName);
    } catch (MalformedObjectNameException e) {
      e.printStackTrace();
    }
  }

  private String createObjectName(String objectName) {
    String tmpObjName = objectName;
    if (!tmpObjName.contains("=")) {
      tmpObjName = "name=" + tmpObjName;
    }
    if (!tmpObjName.contains(":")) {
      tmpObjName = ":" + tmpObjName;
    }
    if (tmpObjName.startsWith(":")) {
      tmpObjName = JMX_DOMAIN + tmpObjName;
    }
    return tmpObjName;
  }
}

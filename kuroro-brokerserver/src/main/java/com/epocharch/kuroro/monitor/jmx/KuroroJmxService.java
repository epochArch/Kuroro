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

import com.alibaba.fastjson.JSONObject;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

public class KuroroJmxService {

  private static Map<String, KuroroJmxService> instanceMaps = new HashMap<String, KuroroJmxService>();
  private JmxClient client;
  private String host;
  private int port;
  private final String defaultDomian = InternalPropKey.JMX_DOMAIN_NAME;

  private KuroroJmxService(String host, int port) {
    this.host = host;
    this.port = port;
    try {
      ensureClient();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static KuroroJmxService getKuroroJmxClientInstance(String host, int port) {
    // 重用链接，有线程安全问题
    KuroroJmxService jmxS = null;
    String insKey = host + String.valueOf(port);
    if (instanceMaps.containsKey(insKey)) {
      jmxS = instanceMaps.get(insKey);
    }
    if (jmxS == null) {
      synchronized (instanceMaps) {
        if (!instanceMaps.containsKey(insKey)) {
          jmxS = new KuroroJmxService(host, port);
          instanceMaps.put(insKey, jmxS);
        }
      }
      jmxS = instanceMaps.get(insKey);
    }
    try {
      jmxS.ensureClient();
      jmxS.client.Connect();
    } catch (IOException e) {
      e.printStackTrace();
      jmxS = new KuroroJmxService(host, port);
      instanceMaps.put(insKey, jmxS);
    } catch (Exception e) {
      jmxS = new KuroroJmxService(host, port);
      instanceMaps.put(insKey, jmxS);
      e.printStackTrace();
    }
    return jmxS;
  }

  private static void echo(String msg) {
    System.out.println(msg);
  }

  public static void main(String[] args) throws IntrospectionException {
    Map<String, Map<String, String>> kvs = new HashMap<String, Map<String, String>>();
    Map<String, String> map1 = new HashMap<String, String>();
    Map<String, String> map2 = new HashMap<String, String>();
    map1.put("aaa", "bb");
    map2.put("aaa", "bb");
    kvs.put("name", map1);
    kvs.put("map", map2);
  }

  private void ensureClient() throws Exception {
    if (this.client == null) {
      this.client = new JmxClient(this.host, this.port);
    }
  }

  public void showJmxBean() throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    String domains[] = mbsc.getDomains();
    for (int i = 0; i < domains.length; i++) {
      echo("/tDomain[" + i + "] = " + domains[i]);
    }

    // Get MBean count
    //
    echo("/nMBean count = " + mbsc.getMBeanCount());

    // Query MBean names
    //
    echo("/nQuery MBeanServer MBeans:");
    Set names = mbsc.queryNames(null, null);
    for (Iterator i = names.iterator(); i.hasNext(); ) {
      echo("/tObjectName = " + (ObjectName) i.next());
    }

    ObjectName stdMBeanName = new ObjectName("TestJMXServer:name=HelloWorld");

    MBeanInfo info = mbsc.getMBeanInfo(stdMBeanName);
    MBeanAttributeInfo[] attr = info.getAttributes();
    for (MBeanAttributeInfo ma : attr) {
      echo("/nName=" + ma.getName());
    }
  }

  public String getAllDomainsObjectName() throws Exception {
    ensureClient();
    Map<String, String> propMaps = new HashMap<String, String>();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    Set names = mbsc.queryNames(null, null);
    if (names.size() > 0) {

      int j = 1;
      for (Iterator i = names.iterator(); i.hasNext(); ) {
        ObjectName on = (ObjectName) i.next();
        propMaps.put("tObjectName" + j + " = ", on.toString());
        j++;
      }

    } else {
      propMaps.put("mssage", "this query not find data");
    }
    return JSONObject.toJSONString(propMaps);
  }

  public String getAllDomains() throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    String domains[] = mbsc.getDomains();
    Map<String, String> propMaps = new HashMap<String, String>();
    if (domains != null && domains.length > 0) {

      for (int i = 0; i < domains.length; i++) {
        propMaps.put("tDomain" + i + "=", domains[i]);
      }

    } else {
      propMaps.put("mssage", "this query not find data");
    }
    return JSONObject.toJSONString(propMaps);

  }

  public Map<String, String> getObjectProperties(ObjectName stdMBeanName) throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    MBeanInfo info = mbsc.getMBeanInfo(stdMBeanName);
    MBeanAttributeInfo[] attr = info.getAttributes();
    if (attr != null && attr.length > 0) {
      Map<String, String> propMaps = new TreeMap<String, String>();
      for (MBeanAttributeInfo ma : attr) {
        propMaps.put(ma.getName(), ma.getName());
      }
      return propMaps;
    }
    return null;

  }

  public Object getDomainObjectNameValue(ObjectName stdMBeanName, String property)
      throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    return mbsc.getAttribute(stdMBeanName, property);
  }

  public String getObjectValues(String domain, String objectName) throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    if (domain == null || "null".equals(domain) || domain.length() <= 0) {
      domain = defaultDomian;
    }
    ObjectName stdMBeanName = new ObjectName(domain + ":name=" + objectName);
    MBeanInfo info = mbsc.getMBeanInfo(stdMBeanName);
    StringBuilder sb = new StringBuilder("");
    MBeanAttributeInfo[] attr = info.getAttributes();
    Map<String, Object> kvs = new TreeMap<String, Object>();
    if (attr != null && attr.length > 0) {
      for (MBeanAttributeInfo ma : attr) {
        String propertyName = ma.getName();
        Object obj = this.getDomainObjectNameValue(stdMBeanName, propertyName);
        if (obj != null) {
          kvs.put(propertyName, obj);
        }
      }
    } else {
      kvs.put("mssage", "this query not find data");
    }
    return JSONObject.toJSONString(kvs);
  }

  public Map<String, Object> getObjectAsMap(String domain, String objectName) throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    if (domain == null || "null".equals(domain) || domain.length() <= 0) {
      domain = defaultDomian;
    }
    ObjectName stdMBeanName = new ObjectName(domain + ":name=" + objectName);
    MBeanInfo info = mbsc.getMBeanInfo(stdMBeanName);
    StringBuilder sb = new StringBuilder("");
    MBeanAttributeInfo[] attr = info.getAttributes();
    Map<String, Object> kvs = new TreeMap<String, Object>();
    if (attr != null && attr.length > 0) {
      for (MBeanAttributeInfo ma : attr) {
        String propertyName = ma.getName();
        Object obj = this.getDomainObjectNameValue(stdMBeanName, propertyName);
        if (obj != null) {
          kvs.put(propertyName, obj);
        }
      }
    }
    return kvs;
  }

  public Map<String, List<String>> getAllConsumers() throws Exception {
    ensureClient();
    Map<String, List<String>> result = new HashMap<String, List<String>>();

    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    Set<ObjectInstance> objectInstanceSet = mbsc
        .queryMBeans(ObjectName.getInstance(defaultDomian + ":name=Consumer-*"), null);

    for (ObjectInstance instance : objectInstanceSet) {
      try {
        Object topicName = mbsc.getAttribute(instance.getObjectName(), "TopicName");
        Object consumerId = mbsc.getAttribute(instance.getObjectName(), "ConsumerId");
        if (topicName != null && consumerId != null) {
          if (!result.containsKey(topicName.toString())) {
            List<String> list = new ArrayList<String>();
            list.add(consumerId.toString());

            result.put(topicName.toString(), list);
          } else {
            result.get(topicName.toString()).add(consumerId.toString());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
    }
    return result;
  }

  public String findObjectsByValue(String domain, String TopicName) throws Exception {
    ensureClient();

    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    Set<ObjectName> names = mbsc.queryNames(null, null);
    Map<String, Object> kvs = new HashMap<String, Object>();
    if (names.size() > 0) {
      Map<String, String> multTopic = new HashMap<String, String>();
      String[] tn = TopicName.split(",");
      for (String t : tn) {
        multTopic.put(t, t);
      }
      List<Map<String, Object>> objectList = new ArrayList<Map<String, Object>>();
      for (Iterator<ObjectName> i = names.iterator(); i.hasNext(); ) {
        ObjectName stdMBeanName = i.next();
        Map<String, String> propMaps = getObjectProperties(stdMBeanName);
        if (propMaps != null && propMaps.containsKey("TopicName")) {
          Object topic = mbsc.getAttribute(stdMBeanName, "TopicName");
          if (topic != null && multTopic.containsKey(topic.toString())) {
            MBeanInfo info = mbsc.getMBeanInfo(stdMBeanName);
            MBeanAttributeInfo[] attr = info.getAttributes();
            if (attr != null && attr.length > 0) {
              // Map<String,Object> kvs = new
              // HashMap<String,Object>();
              for (MBeanAttributeInfo ma : attr) {
                String propertyName = ma.getName();
                Object obj = this.getDomainObjectNameValue(stdMBeanName, propertyName);
                if (obj != null) {
                  kvs.put(propertyName, obj);
                }
              }
              objectList.add(kvs);
            }
          }
        }
      }
      if (objectList.size() > 0) {
        return JSONObject.toJSONString(objectList);
      }

    }
    kvs.put("mssage", "this query not find data");
    return JSONObject.toJSONString(kvs);

  }

  public void toString(List<String> list) {

  }

  public String getCompensating() throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    String domain = InternalPropKey.JMX_DOMAIN_NAME;
    ObjectName stdMBeanName = new ObjectName(domain + ":name=Compensator");
    Object value = mbsc.getAttribute(stdMBeanName, "Compensating");

    return value == null ? "" : value.toString();
  }

  public boolean isCompensatorStarted() throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    ObjectName stdMBeanName = new ObjectName(defaultDomian + ":name=Compensator");
    Object value = mbsc.getAttribute(stdMBeanName, "Status");

    return value == null ? false : Boolean.parseBoolean(value.toString());
  }

  public String invokeMethod(String method, String topic, String consumerId) throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    ObjectName stdMBeanName = new ObjectName(defaultDomian + ":name=Compensator");

    return mbsc.invoke(stdMBeanName, method, new Object[]{topic, consumerId},
        new String[]{String.class.getName(), String.class.getName()}).toString();
  }

  public String invokeMethod(String method, String topic, String consumerId, String zone)
      throws Exception {
    ensureClient();
    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    ObjectName stdMBeanName = new ObjectName(defaultDomian + ":name=Compensator");

    return mbsc.invoke(stdMBeanName, method, new Object[]{topic, consumerId, zone},
        new String[]{String.class.getName(), String.class.getName(), String.class.getName()})
        .toString();
  }

  /**
   * @author zhoushugang
   */
  public String invokeConsumerOper(String method, String topic, String consumerId)
      throws Exception {
    ensureClient();
      if (KuroroUtil.isBlankString(method) || KuroroUtil.isBlankString(topic)
          || KuroroUtil.isBlankString(consumerId)) {
        return "";
      }

    MBeanServerConnection mbsc = this.client.getMbeanServerConnection();
    ObjectName stdMBeanName = new ObjectName(defaultDomian + ":name=Consumer-" + topic + '-' + consumerId);

    return mbsc.invoke(stdMBeanName, method, new Object[]{topic, consumerId},
        new String[]{String.class.getName(), String.class.getName()}).toString();
  }

  public void closeConnection() {
    try {
      this.client.Close();
      this.client = null;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

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

package com.epocharch.kuroro.monitor.support.switcher;

import com.alibaba.fastjson.JSONObject;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.monitor.jmx.KuroroJmxService;
import com.epocharch.kuroro.monitor.support.BaseSwitcher;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class JMXQuerySwitcher extends BaseSwitcher {

  public static String name = "jmx";

  public static final String defaultDomain = InternalPropKey.JMX_DOMAIN_NAME;

  @Override
  public String regist() {
    return name;
  }

  @Override
  public Object logic(Map<String, String[]> params) {
    return processJMXQuery(params);
  }

  @Override
  public String urlDesc() {
    return "flog.do?switch=jmx&host=localhost&command=d";
  }

  @Override
  public Object status() {
    return "OK";
  }

  private Object processJMXQuery(Map<String, String[]> params) {
    JSONObject json = new JSONObject();
    String host = this.getParamter("host", params);
    String port = this.getParamter("port", params);
    String content = null;
    Map<String, String> propMaps = new HashMap<String, String>();
    if (StringUtils.isEmpty(host)) {

      content = "parameter host is not null";
      json.put("result", "fail");
      json.put("info", content);
      return json;
    }
    if (StringUtils.isEmpty(port)) {
      port = "3998";
    }

    if (content == null) {
      KuroroJmxService jjs = KuroroJmxService
          .getKuroroJmxClientInstance(host, Integer.parseInt(port));
      String command = this.getParamter("command", params);
      String objectName = this.getParamter("object", params);
      if (StringUtils.isEmpty(command) && !StringUtils.isEmpty(objectName)) {
        command = "p";
      } else if (StringUtils.isEmpty(command)) {
        command = "topic";
      }
      if ("d".equals(command)) {
        try {
          content = jjs.getAllDomains();
          json.put("result", "success");
          json.put("info", content);
          return json;
        } catch (Exception e) {
          e.printStackTrace();
          json.put("result", "fail");
          json.put("info", e.getMessage());
          return json;
        }

      } else if ("o".equals(command)) {
        try {
          content = jjs.getAllDomainsObjectName();
          json.put("result", "success");
          json.put("info", content);
          return json;
        } catch (Exception e) {
          e.printStackTrace();
          json.put("result", "fail");
          json.put("info", e.getMessage());
          return json;
        }

      } else if ("p".equals(command)) {
        String domain = this.getParamter("domain", params);
        if (objectName == null || objectName.length() <= 0) {
          content = "while parameter command value is properties，"
              + "the parameter object is not null,the domain parameter default value is " + defaultDomain;
          json.put("result", "fail");
          json.put("info", content);
          return json;
        }
        try {
          content = jjs.getObjectValues(domain, objectName);
          json.put("result", "success");
          json.put("info", content);
          return json;
        } catch (Exception e) {
          e.printStackTrace();
          json.put("result", "fail");
          json.put("info", e.getMessage());
          return json;
        }

      } else if ("topic".equals(command)) {
        String topic = this.getParamter("topic", params);
        String domain = this.getParamter("domain", params);
        if (topic == null || topic.length() <= 0) {
          content = "while parameter command value is topic，"
              + "the parameter topicName is not null,the domain parameter default value is " + defaultDomain;
          json.put("result", "fail");
          json.put("info", content);
          return json;
        }
        try {
          content = jjs.findObjectsByValue(domain, topic);
          json.put("result", "success");
          json.put("info", content);
          return json;
        } catch (Exception e) {
          e.printStackTrace();
          json.put("result", "fail");
          json.put("info", e.getMessage());
          return json;
        }

      } else {

        content = "parameter command value is [domains,objects,property,topic],default is topic,others is unvalid";
        json.put("result", "fail");
        json.put("info", content);
        return json;
      }
    }
    return json;

  }
}

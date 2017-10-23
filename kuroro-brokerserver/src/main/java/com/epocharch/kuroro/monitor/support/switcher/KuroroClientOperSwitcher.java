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
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.monitor.service.impl.KuroroServiceImpl;
import com.epocharch.kuroro.monitor.support.BaseSwitcher;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class KuroroClientOperSwitcher extends BaseSwitcher {

  public static String name = "consumerJMXOper";

  @Override
  public String regist() {
    return name;
  }

  //调用consumer jmx的开关逻辑
  @Override
  public Object logic(Map<String, String[]> params) {
    String method = params.get("m")[0];
    String topic = params.get("topic")[0];
    String consumerId = params.get("consumerId")[0];
    JSONObject json = new JSONObject();
    if (method != null && !method.isEmpty() && topic != null && !topic.isEmpty()
        && consumerId != null && !consumerId.isEmpty()) {
      try {
        ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
        String topicPath = KuroroUtil.getChildFullPath(KuroroServiceImpl.KURORO_TOPIC_PATH, topic);
        if (zkClient.exists(topicPath)) {
          TopicConfigDataMeta meta = zkClient.readData(topicPath);
          if (meta != null) {
            if ((method.equals("consumerStart"))) {
              if (isContainsConsumerid(meta, consumerId)) {
                List<String> suspendConsumerIdList = meta.getSuspendConsumerIdList();
                suspendConsumerIdList.remove(consumerId);
                zkClient.writeData(topicPath, meta);
              }
            } else if (method.equals("consumerStop")) {
              if (!isContainsConsumerid(meta, consumerId)) {
                if (meta.getSuspendConsumerIdList() == null) {
                  List<String> suspendConsumerIdList = new ArrayList<String>();
                  suspendConsumerIdList.add(consumerId);
                  meta.setSuspendConsumerIdList(suspendConsumerIdList);
                } else {
                  List<String> suspendConsumerIdList = meta.getSuspendConsumerIdList();
                  suspendConsumerIdList.add(consumerId);
                }
                zkClient.writeData(topicPath, meta);
              }
            }
          }
        }

        json.put("result", "success");
        json.put("info", JSONObject.toJSONString(method + "--" + topic + "--" + consumerId));
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      json.put("result", "fail");
      json.put("info", StringUtils.EMPTY);
      return json;
    }
    return json;
  }

  @Override
  public String urlDesc() {
    return "flog.do?switch=consumerJMXOper&m=consumerStart|consumerStop&topic=&consumerId=";
  }

  @Override
  public Object status() {
    return "OK";
  }

  private boolean isContainsConsumerid(TopicConfigDataMeta meta, String id) {
    if (meta.getSuspendConsumerIdList() != null && meta.getSuspendConsumerIdList().contains(id)) {
      return true;
    } else {
      return false;
    }
  }
}

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;

public class KuroroConsumerFlowControl extends BaseSwitcher {

  public static String name = "kuroroConsumerFlowControl";

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
    String flowControlSize = params.get("flowControlSize")[0];
    JSONObject json = new JSONObject();
    if (method != null && !method.isEmpty() && topic != null && !topic.isEmpty()
        && consumerId != null && !consumerId.isEmpty()
        && flowControlSize != null && !flowControlSize.isEmpty()) {
      try {
        ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
        String topicPath = KuroroUtil.getChildFullPath(KuroroServiceImpl.KURORO_TOPIC_PATH, topic);
        if (zkClient.exists(topicPath)) {
          TopicConfigDataMeta meta = zkClient.readData(topicPath);
          if (meta != null) {
            if ((method.equals("stopConsumerFlowControl"))) {
              if (isContainsConsumerid(meta, consumerId)) {
                ConcurrentHashMap<String, Integer> flowConsumerIdMap = meta.getFlowConsumerIdMap();
                flowConsumerIdMap.remove(consumerId);
                zkClient.writeData(topicPath, meta);
              }
            } else if (method.equals("startConsumerFlowControl")) {
              if (!isContainsConsumerid(meta, consumerId)) {
                if (meta.getFlowConsumerIdMap() == null) {
                  ConcurrentHashMap<String, Integer> flowConsumerIdMap = new ConcurrentHashMap<String, Integer>();
                  flowConsumerIdMap.put(consumerId, Integer.valueOf(flowControlSize));
                  meta.setFlowConsumerIdMap(flowConsumerIdMap);
                } else {
                  ConcurrentHashMap<String, Integer> flowConsumerIdMap = meta
                      .getFlowConsumerIdMap();
                  flowConsumerIdMap.put(consumerId, Integer.valueOf(flowControlSize));
                }
              } else {
                ConcurrentHashMap<String, Integer> flowConsumerIdMap = meta.getFlowConsumerIdMap();
                flowConsumerIdMap.put(consumerId, Integer.valueOf(flowControlSize));
              }
              zkClient.writeData(topicPath, meta);
            }
          }
        }

        json.put("result", "success");
        json.put("info", JSONObject
            .toJSONString(method + "--" + topic + "--" + consumerId + "--" + flowControlSize));
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
    return "flog.do?switch=kuroroConsumerFlowControl&m=startConsumerFlowControl|stopConsumerFlowControl&topic=&consumerId=&flowControlSize=";
  }

  @Override
  public Object status() {
    return "OK";
  }

  private boolean isContainsConsumerid(TopicConfigDataMeta meta, String id) {
    if (meta.getFlowConsumerIdMap() != null && meta.getFlowConsumerIdMap().containsKey(id)) {
      return true;
    } else {
      return false;
    }
  }
}

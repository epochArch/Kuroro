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
import com.epocharch.kuroro.common.inner.config.impl.TransferConfig;
import com.epocharch.kuroro.monitor.support.BaseSwitcher;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import com.epocharch.zkclient.ZkClient;
import java.util.HashMap;
import java.util.Map;


public class KuroroTransferConfigSwitcher extends BaseSwitcher {

  public static final String name = "transconfig";
  public static final String TRANSFER_CONFIG_PATH = MonitorZkUtil.IDCZkTransferConfigPath;
  public final ZkClient client = MonitorZkUtil.getLocalIDCZk();

  @Override
  public String regist() {
    if (!client.exists(TRANSFER_CONFIG_PATH)) {
      client.createPersistent(TRANSFER_CONFIG_PATH);
    }
    return name;
  }

  @Override
  public Object logic(Map<String, String[]> params) {
    JSONObject result = new JSONObject();

    if (params != null) {
      String commond = this.getParamter("m", params);
      if (commond != null && commond.equals("modify")) {
        try {
          TransferConfig tcMetaData = null;
          try {
            tcMetaData = MonitorZkUtil.getLocalIDCZk().readData(TRANSFER_CONFIG_PATH);
          } catch (Exception e) {
            e.printStackTrace();
          }
          if (tcMetaData == null) {
            tcMetaData = new TransferConfig();
            tcMetaData.setConsumerNums(2);
            tcMetaData.setTopicConsumerThreadNums(1);
            tcMetaData.setTopicQueueTopBottom(150);
            tcMetaData.setTopicQueueTopBottom(50);
            tcMetaData.setTransferBatchProducorOff(true);
            tcMetaData.setTransferWorkerThreadNums(10);
            tcMetaData.setTransferWorkerProcessLevel(50);
            tcMetaData.setZoneThrottlerMap(new HashMap<String, Float>());
            tcMetaData.addZoneThrottle("ZONE_NH", 0.1F);
            tcMetaData.addZoneThrottle("ZONE_JQ", 0.1F);
          }
          String consumerNums = this.getParamter("cns", params);
          String consumerPoolSize = this.getParamter("cps", params);
          String workThreadNums = this.getParamter("wtns", params);
          String tqmax = this.getParamter("tqmax", params);
          String tqmin = this.getParamter("tqmin", params);
          String n2j = this.getParamter("N2J", params);
          String j2n = this.getParamter("J2N", params);
          if (consumerNums != null && consumerNums.length() > 0) {
            tcMetaData.setConsumerNums(Integer.parseInt(consumerNums));
          }
          if (consumerPoolSize != null && consumerPoolSize.length() > 0) {
            tcMetaData.setTopicConsumerThreadNums(Integer.parseInt(consumerPoolSize));
          }
          if (workThreadNums != null && workThreadNums.length() > 0) {
            tcMetaData.setTransferWorkerThreadNums(Integer.parseInt(workThreadNums));
          }
          if (tqmax != null && tqmax.length() > 0) {
            tcMetaData.setTopicQueueTopLimit(Integer.parseInt(tqmax));
          }
          if (tqmin != null && tqmin.length() > 0) {
            tcMetaData.setTopicQueueTopBottom(Integer.parseInt(tqmin));
          }
          if (n2j != null && n2j.length() > 0) {
            tcMetaData.addZoneThrottle("ZONE_NH", Float.parseFloat(n2j));
          }
          if (j2n != null && j2n.length() > 0) {
            tcMetaData.addZoneThrottle("ZONE_JQ", Float.parseFloat(j2n));
          }

          MonitorZkUtil.getLocalIDCZk().writeData(TRANSFER_CONFIG_PATH, tcMetaData);
          result.put("result", "success");
          result.put("info",
              JSONObject.toJSONString(client.readData(TRANSFER_CONFIG_PATH)));
          return result;

        } catch (Exception e1) {
          e1.printStackTrace();
          result.put("result", "fail");
          result.put("info", e1.getMessage());
          return result;
        }

      } else if (commond != null && commond.equals("view")) {
        TransferConfig tcMetaData = null;
        try {
          tcMetaData = client.readData(TRANSFER_CONFIG_PATH);
          result.put("result", "success");
          result.put("info", JSONObject.toJSONString(tcMetaData));
          return result;
        } catch (Exception e) {
          e.printStackTrace();
          result.put("result", "fail");
          result.put("info", e.getMessage());
          return result;
        }
      }
    }
    result.put("result", "fail");
    result.put("info", "param is null ");
    return result;
  }

  @Override
  public String urlDesc() {
    /**
     * cns consumer的个数 cps 消费线程池的大小 wtns 生产线程的个数 tqmax topic的消费队列容量的最大个数
     * tqmin topic的消费队列容量的最小个数 N2J 南汇到金桥的流量比 J2N 金桥到南汇的流量比
     */

    return "flog.do?switch=transconfig&m=modify,view&cns=2&cps=3&wtns=10&tqmax=100&tqmin=50&N2J=0.01&J2N=0.01";
  }

  @Override
  public Object status() {
    TransferConfig tcMetaData = null;
    try {
      tcMetaData = client.readData(TRANSFER_CONFIG_PATH);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (tcMetaData == null) {
      return "ERROR";
    }
    return tcMetaData;
  }

}

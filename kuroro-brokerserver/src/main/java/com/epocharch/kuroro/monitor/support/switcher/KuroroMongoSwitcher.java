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
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.monitor.support.BaseSwitcher;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.keyvalue.DefaultKeyValue;
import org.apache.commons.lang.StringUtils;

/**
 * 新增url开关
 *
 * @author zhangjin
 */
public class KuroroMongoSwitcher extends BaseSwitcher {

  private final static String KURORO_MONGO_PATH = MonitorZkUtil.IDCZkMongoPath;
  static String kuroroMongo = "kuroroMongo";

  /**
   * 更改MQ发送统计数据的url
   *
   * @param method 方法
   * @param index 排序
   * @param monGo 地址
   */
  public static Object changeKuroroMongo(String method, String index, String monGo) {
    List<DefaultKeyValue> list = new ArrayList<DefaultKeyValue>();
    JSONObject json = new JSONObject();
    String path = KURORO_MONGO_PATH + "/" + index;
    try {
        if (KuroroUtil.isBlankString(method) || KuroroUtil.isBlankString(index) || KuroroUtil.isBlankString(monGo)) {
          json.put("result", "success");json.put("info", JSONObject.toJSONString(list));
          return json;
      }

      ZkClient zk = KuroroZkUtil.initIDCZk();
      if (!zk.exists(KURORO_MONGO_PATH)) {
        zk.createPersistent(KURORO_MONGO_PATH);
      }
      if (StringUtils.equals("add", method)) {
        if (zk.exists(path)) {
          zk.delete(path);
        }
        zk.createPersistent(path, monGo);
      } else if (StringUtils.equals("del", method)) {
        if (zk.exists(path)) {
          zk.delete(path);
        }
      }

      List<String> children = zk.getChildren(KURORO_MONGO_PATH);
      for (String child : children) {
        String mongo = zk.readData(KURORO_MONGO_PATH + "/" + child);
        list.add(new DefaultKeyValue(child, mongo));
      }
      json.put("result", "success");
      json.put("info", JSONObject.toJSONString(list));
      return json;
    } catch (Exception e) {
      logger.error("write KURORO_MONGO error.", e);
      json.put("result", "fail");
      json.put("info", e.getMessage());
      return json;
    }
  }

  @Override
  public String regist() {
    // MQ的mongo配置
    try {
      if (!MonitorZkUtil.getLocalIDCZk().exists(KURORO_MONGO_PATH)) {
        MonitorZkUtil.getLocalIDCZk().createPersistent(KURORO_MONGO_PATH, true);
      }
    } catch (Exception e) {

    }

    return kuroroMongo;
  }

  /**
   * 参数说明:
   * <p>
   * 1. add,url
   * <p>
   * 2. del,url
   */
  @Override
  public Object logic(Map<String, String[]> params) {
    JSONObject json = new JSONObject();
    if (params != null) {
      String command = this.getParamter("m", params);
      String index = this.getParamter("i", params);
      String monGo = this.getParamter("g", params);
      return changeKuroroMongo(command, index, monGo);
    }
    json.put("result", "fail");
    json.put("info", StringUtils.EMPTY);
    return json;
  }

  @Override
  public String urlDesc() {
    return "flog.do?switch=kuroroMongo&m=add,del&index=&g=";
  }

  @Override
  public Object status() {
    try {
      return "";
    } catch (Exception e) {
      logger.error(e);
    }
    return StringUtils.EMPTY;
  }
}
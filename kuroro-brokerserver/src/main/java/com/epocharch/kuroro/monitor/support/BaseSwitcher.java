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

package com.epocharch.kuroro.monitor.support;

import com.epocharch.kuroro.monitor.support.switcher.JMXQuerySwitcher;
import com.epocharch.kuroro.monitor.support.switcher.KuroroClientOperSwitcher;
import com.epocharch.kuroro.monitor.support.switcher.KuroroConsumerFlowControl;
import com.epocharch.kuroro.monitor.support.switcher.KuroroMongoSwitcher;
import com.epocharch.kuroro.monitor.support.switcher.KuroroTransferConfigSwitcher;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *  开关接口
 *
 * @author dongheng
 */

public abstract class BaseSwitcher {

  protected static final Log logger = LogFactory.getLog(BaseSwitcher.class);
  protected static final Map<String, BaseSwitcher> switchers = new Hashtable<String, BaseSwitcher>() {
    private static final long serialVersionUID = 1L;

    {
      regist(this, new JMXQuerySwitcher());
      regist(this, new KuroroMongoSwitcher());
      regist(this, new KuroroTransferConfigSwitcher());
      regist(this, new KuroroClientOperSwitcher());
      regist(this, new KuroroConsumerFlowControl());
    }
  };
  protected static String comma = "，";

  public static void regist(Map<String, BaseSwitcher> map, BaseSwitcher detectorSwitcher) {
    map.put(detectorSwitcher.regist(), detectorSwitcher);
  }

  public static BaseSwitcher getBaseSwitcher(String switchName) {
    if (StringUtils.isEmpty(switchName)) {
      return null;
    }

    return switchers.get(switchName);
  }

  public static Collection<BaseSwitcher> init() {
    return switchers.values();
  }

  /**
   * 注册开关， 返回开关名称
   */
  public abstract String regist();

  /**
   * 开关逻辑
   */
  public abstract Object logic(Map<String, String[]> params);

  /**
   * 开关使用描述
   */
  public abstract String urlDesc();

  /**
   * 开关状态信息
   */
  public abstract Object status();

  public String getParamter(String key, Map<String, String[]> params) {
    if (params != null) {
      if (params.containsKey(key) && params.get(key) != null) {
        return params.get(key)[0];
      }
    }

    return null;
  }
}

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

package com.epocharch.kuroro.monitor.plugin;

import com.epocharch.common.config.PropertiesContainer;
import com.epocharch.kuroro.broker.constants.BrokerInConstants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.message.MessageLog;
import com.epocharch.kuroro.monitor.common.DateUtil;
import com.epocharch.kuroro.monitor.dto.TimeInterval;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

/**
 * 分析日志的工具类
 *
 * @author dongheng
 */
public class AnalyseUtil {

  private static final Logger logger = Logger.getLogger(AnalyseUtil.class);
  /**
   * key:AnalysePluginClass,<key:time,value:Analyse Result>
   **/
  static Map<Class<?>, Map<String, AnalysePlugin>> analyseMap = new ConcurrentHashMap<Class<?>, Map<String, AnalysePlugin>>();
  private static ArrayList<String> formerKeyTime = new ArrayList<String>();

  /**
   * 分析MessageInfo监控信息
   */
  public static void analyseKuroroInfo(Object messageInfo) {
    try {
      MessageLog messageLog = (MessageLog) messageInfo;
      // 1.循环遍历
      for (AnalysePlugin analysePlugin : AnalysePlugin.messagePlugin) {
        if (analysePlugin.accept(messageInfo)) {
          Date startTime = TimeInterval
              .getStartTime(messageLog.fetchAnalystTime(), analysePlugin.getTimeInterval());
          AnalysePlugin plugin = getMemoInstance(analysePlugin, startTime);
          if (plugin != null) {
            plugin.analyse(messageInfo);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("analyseKuroroInfo error.", e);
    }
  }

  /**
   * 分析客户端日志方法
   */
  public static void analyseServerBizLog(Object serverBizLog) {

  }

  /**
   * 获取内存统计对象
   */
  public static AnalysePlugin getMemoInstance(AnalysePlugin analysePlugin, Date startTime) {
    String timeKey = DateUtil.getMinsStartString(startTime);
    Map<String, AnalysePlugin> pluginMap = analyseMap.get(analysePlugin.getClass());
    String env = PropertiesContainer.getInstance().getProperty(BrokerInConstants.KURORO_BORKER_NAMESPACE,
        InternalPropKey.CURRENT_ENV);
    //测试环境判断内存中的数据是否达到规定的上限

    if (pluginMap == null) {
      pluginMap = new ConcurrentHashMap<String, AnalysePlugin>();
      AnalysePlugin newPlugin = (AnalysePlugin) analysePlugin.createInstance();
      pluginMap.put(timeKey, analysePlugin);
      analyseMap.put(newPlugin.getClass(), pluginMap);
      return analysePlugin;
    }
    if (env.equals("test") && analysePlugin instanceof KuroroInfoAnalysePlugin) {
      if (pluginMap.size() > 5) {
        if (!formerKeyTime.contains(timeKey)) {
          formerKeyTime.add(timeKey);
        }
        return null;
      } else {
        if (formerKeyTime.size() >= 5) {
          formerKeyTime.remove(0);
        }
      }
    }
    if (!pluginMap.containsKey(timeKey) && !formerKeyTime.contains(timeKey)) {
      AnalysePlugin newPlugin = (AnalysePlugin) analysePlugin.createInstance();
      pluginMap.put(timeKey, newPlugin);
      return analysePlugin;
    }

    return pluginMap.get(timeKey);
  }

  public static Map<Class<?>, Map<String, AnalysePlugin>> getAnalyseMap() {
    return analyseMap;
  }

  public static void setAnalyseMap(Map<Class<?>, Map<String, AnalysePlugin>> analyseMap) {
    AnalyseUtil.analyseMap = analyseMap;
  }
}

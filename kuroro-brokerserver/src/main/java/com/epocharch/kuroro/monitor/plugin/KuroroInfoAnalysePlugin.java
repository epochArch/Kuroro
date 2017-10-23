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


import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.message.MessageLog;
import com.epocharch.kuroro.monitor.dto.KuroroInOutAnalyse;
import com.epocharch.kuroro.monitor.dto.KuroroStatistics;
import com.epocharch.kuroro.monitor.dto.TimeInterval;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQInfo流式分析插件
 *
 * @author dongheng
 */
public class KuroroInfoAnalysePlugin implements AnalysePlugin {

  protected Date startTime;
  protected Date endTime;
  protected TimeInterval timeInterval = TimeInterval.ONE_MINS;
  /**
   * topicName为key
   */
  private Map<String, KuroroStatistics> statisticsMap = new ConcurrentHashMap<String, KuroroStatistics>();

  public KuroroInfoAnalysePlugin() {
    timeInterval = TimeInterval.ONE_MINS;
    startTime = TimeInterval.getStartTime(new Date(), getTimeInterval());
    endTime = new Date(startTime.getTime() + timeInterval.getCode() * 60 * 1000);
    register();
  }

  public Date getStartTime() {
    return startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public TimeInterval getTimeInterval() {
    return timeInterval;
  }

  /**
   * 系统在启动的时候 要注册所有插件进来
   */
  public void register() {
    if (pluginSet.contains(this.getClass())) {
      return;
    }
    pluginSet.add(this.getClass());
    messagePlugin.add(this);
  }

  public void setTimes(Object object) {
    MessageLog messageLog = (MessageLog) object;

    startTime = TimeInterval.getStartTime(messageLog.fetchAnalystTime(), getTimeInterval());
    endTime = new Date(startTime.getTime() + timeInterval.getCode() * 60 * 1000);
  }

  @Override
  public boolean accept(Object object) {
    return true;
  }

  @Override
  public AnalysePlugin analyse(Object object) {
    MessageLog messageLog = (MessageLog) object;
    getKuroroInfo(messageLog.getTopicName(), messageLog.getZoneName()).statistics(messageLog);
    return this;
  }

  public KuroroStatistics getKuroroInfo(String topicName, String zoneName) {
    String key = null;
    if (!KuroroUtil.isBlankString(zoneName)) {
      key = topicName + "#" + zoneName;
    } else {
      key = topicName;
    }
    KuroroStatistics statistics = statisticsMap.get(key);
    if (statistics != null) {
      return statistics;
    }
    statistics = new KuroroStatistics();
    statistics.setTopicName(topicName);
    statistics.setZoneName(zoneName);
    statisticsMap.put(key, statistics);
    return statistics;
  }

  @Override
  public Object toEntity() {
    List<KuroroInOutAnalyse> result = new ArrayList<KuroroInOutAnalyse>();
    for (Entry<String, KuroroStatistics> entry : statisticsMap.entrySet()) {
      KuroroInOutAnalyse kuroroInOutAnalyse = entry.getValue().toEntity();
      kuroroInOutAnalyse.setTimes(getStartTime(), getEndTime());
      result.add(kuroroInOutAnalyse);
    }
    return result;
  }

  @Override
  public AnalysePlugin createInstance() {
    return new KuroroInfoAnalysePlugin();
  }
}

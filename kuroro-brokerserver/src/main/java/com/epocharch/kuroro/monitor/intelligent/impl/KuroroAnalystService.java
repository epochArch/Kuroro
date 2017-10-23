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

package com.epocharch.kuroro.monitor.intelligent.impl;

import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.monitor.common.DateUtil;
import com.epocharch.kuroro.monitor.dao.impl.BaseMybatisDAOImpl;
import com.epocharch.kuroro.monitor.dto.ChartsData;
import com.epocharch.kuroro.monitor.dto.KuroroParam;
import com.epocharch.kuroro.monitor.dto.KuroroSumInfo;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

/**
 * mq的业务统计分析类
 *
 * @author dongheng
 */
public class KuroroAnalystService {

  static final Integer oneHour = 60 * 60 * 1000;
  static final Integer oneDay = 24 * 60 * 60 * 1000;
  static final Map<String, Set<String>> combox = new HashMap<String, Set<String>>();
  static final String topics = "topics";
  static final String brokers = "brokers";
  static final String at = "@";
  static final String zones = "zones";
  static final String consumers = "consumers";
  static final String consumerIps = "consumerIp";
  static final String connPorts = "connPort";
  static final AtomicLong ATOMIC_LONG = new AtomicLong(0);
  private static final Logger logger = Logger.getLogger(AnalystJobService.class);
  private static final long oneMins = 60 * 1000;
  private static final long sixteenMins = 16 * 60 * 1000;
  private BaseMybatisDAOImpl memoMyIbaitsDAO;
  private BaseMybatisDAOImpl analyseMyIbaitsDAO;
  private String[] timeUnit = {"hour", "day"};

  public void setMemoMyIbaitsDAO(BaseMybatisDAOImpl memoMyIbaitsDAO) {
    this.memoMyIbaitsDAO = memoMyIbaitsDAO;
  }

  public void setAnalyseMyIbaitsDAO(BaseMybatisDAOImpl analyseMyIbaitsDAO) {
    this.analyseMyIbaitsDAO = analyseMyIbaitsDAO;
  }


  /**
   * 获取前一刻钟时间
   */
  public static List<Object> getQuarterMins() {
    List<Object> result = new ArrayList<Object>();
    long minsAgo = System.currentTimeMillis() - sixteenMins;
    for (int i = 0; i < 15; i++) {
      result.add(DateUtil.getHourMins(new Date(minsAgo + i * 60 * 1000)));
    }
    return result;
  }

  /**
   * 获取kuroro分钟级的进出统计数据
   */
  @SuppressWarnings("rawtypes")
  public ChartsData getMinsInOutCounts(KuroroParam kuroroParam) {
    ChartsData result = new ChartsData();
    if (kuroroParam == null || kuroroParam.getTopicName() == null) {
      return result;
    }
    kuroroParam.setStartTime(new Date(System.currentTimeMillis() - sixteenMins));
    kuroroParam.setEndTime(new Date());
    List<HashMap> outList = memoMyIbaitsDAO
        .queryForList("monitor_kuroro_consumer_analyse.getOutMins", kuroroParam, HashMap.class);
    List<HashMap> inList = memoMyIbaitsDAO
        .queryForList("monitor_kuroro_producer_analyse.getInMins", kuroroParam, HashMap.class);
    Map<String, Integer> outMap = new HashMap<String, Integer>();

    for (HashMap curt : outList) {
      Date startTime = (Date) curt.get("start_time");
      String key = DateUtil.getHourMins(startTime);
      Number outCounts = (Number) curt.get("outCounts");
      if (outCounts != null) {
        outMap.put(key, outCounts.intValue());
      }
    }

    Map<String, Integer> inMap = new HashMap<String, Integer>();
    for (HashMap curt : inList) {
      Date startTime = (Date) curt.get("start_time");
      String key = DateUtil.getHourMins(startTime);
      Number inCounts = (Number) curt.get("inCounts");
      if (inCounts != null) {
        inMap.put(key, inCounts.intValue());
      }
    }

    List<Object> hourMins = getQuarterMins();
    List<Object> inCounts = new ArrayList<Object>();
    List<Object> outCounts = new ArrayList<Object>();
    int totalIn = 0;
    int totalOut = 0;
    for (Object object : hourMins) {
      String key = (String) object;
      if (inMap.containsKey(key)) {
        inCounts.add(inMap.get(key));
        totalIn = totalIn + inMap.get(key);
      } else {
        inCounts.add(0);
      }

      if (outMap.containsKey(key)) {
        outCounts.add(outMap.get(key));
        totalOut = totalOut + outMap.get(key);
      } else {
        outCounts.add(0);
      }
    }

    result.setXdata(hourMins);
    result.setYdata(inCounts);
    result.setOthers(outCounts);
    List<Object> extInfo = new ArrayList<Object>();
    extInfo.add(totalIn);
    extInfo.add(totalOut);
    result.setExtInfo(extInfo);
    return result;
  }

  /**
   * Kuroro 历史报表数据查询
   */
  public ChartsData getHistInOutCounts(KuroroParam kuroroParam) {
    ChartsData result = new ChartsData();
    if (kuroroParam == null || kuroroParam.getTopicName() == null) {
      return result;
    }

    List<HashMap> outList = analyseMyIbaitsDAO
        .queryForList("monitor_kuroro_consumer_analyse.getHisData", kuroroParam, HashMap.class);
    List<HashMap> inList = analyseMyIbaitsDAO
        .queryForList("monitor_kuroro_producer_analyse.getHisData", kuroroParam, HashMap.class);

    LinkedHashMap<String, Integer> outMap = new LinkedHashMap<String, Integer>();
    for (HashMap curt : outList) {
      Date startTime = (Date) curt.get("start_time");
      String key = DateUtil.getHourMins(startTime);
      Number outCounts = (Number) curt.get("outCounts");
      if (outCounts != null) {
        outMap.put(key, outCounts.intValue());
      }
    }

    LinkedHashMap<String, Integer> inMap = new LinkedHashMap<String, Integer>();
    for (HashMap curt : inList) {
      Date startTime = (Date) curt.get("start_time");
      String key = DateUtil.getHourMins(startTime);
      Number inCounts = (Number) curt.get("inCounts");
      if (inCounts != null) {
        inMap.put(key, inCounts.intValue());
      }
    }
    List<Object> outHourMins = new ArrayList<Object>();

    Set set = outMap.entrySet();
    Iterator it = set.iterator();
    while (it.hasNext()) {
      Map.Entry me = (Map.Entry) it.next();
      outHourMins.add(me.getKey());
    }

    List<Object> inHourMins = new ArrayList<Object>();

    Set inSet = inMap.entrySet();
    Iterator inIt = inSet.iterator();
    while (inIt.hasNext()) {
      Map.Entry me = (Map.Entry) inIt.next();
      inHourMins.add(me.getKey());
    }

    List<Object> hourMins = new ArrayList<Object>();
    if (outHourMins.size() > 0) {
      hourMins = outHourMins;
    } else {
      hourMins = inHourMins;
    }

    List<Object> inCounts = new ArrayList<Object>();
    List<Object> outCounts = new ArrayList<Object>();
    int totalIn = 0;
    int totalOut = 0;
    for (Object object : hourMins) {
      String key = (String) object;
      if (inMap.containsKey(key)) {
        inCounts.add(inMap.get(key));
        totalIn = totalIn + inMap.get(key);
      } else {
        inCounts.add(0);
      }

      if (outMap.containsKey(key)) {
        outCounts.add(outMap.get(key));
        totalOut = totalOut + outMap.get(key);
      } else {
        outCounts.add(0);
      }
    }

    result.setXdata(hourMins);
    result.setYdata(inCounts);
    result.setOthers(outCounts);
    List<Object> extInfo = new ArrayList<Object>();
    extInfo.add(totalIn);
    extInfo.add(totalOut);
    result.setExtInfo(extInfo);
    return result;
  }

  @SuppressWarnings("rawtypes")
  public void handInCount(List<HashMap> list, Map<String, KuroroSumInfo> sums) {
    if (list != null) {
      for (HashMap map : list) {
        String topicName = (String) map.get("topic_name");
        Number sumCounts = (Number) map.get("sumCounts");
        KuroroSumInfo sumInfo = sums.get(topicName);
        if (sumInfo == null) {
          sumInfo = new KuroroSumInfo();
          sumInfo.setTopicName(topicName);
          sums.put(topicName, sumInfo);
        }
        sumInfo.addInCounts(sumCounts.intValue());
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public void handOutCount(List<HashMap> list, Map<String, KuroroSumInfo> sums) {
    if (list != null) {
      for (HashMap map : list) {
        String topicName = (String) map.get("topic_name");
        String consumerName = (String) map.get("consumer_name");
        Number sumCounts = (Number) map.get("sumCounts");
        KuroroSumInfo sumInfo = sums.get(topicName + consumerName);
        if (sumInfo == null) {
          sumInfo = new KuroroSumInfo();
          sumInfo.setTopicName(topicName);
          sumInfo.setConsumerName(consumerName);
          sums.put(topicName + consumerName, sumInfo);
        }
        sumInfo.addOutCounts(sumCounts.intValue());
      }
    }
  }


  /**
   * 获取所有topic生产量的统计信息
   */
  public List<KuroroSumInfo> producerDaySum(KuroroParam params) {
    List<KuroroSumInfo> result = new ArrayList<KuroroSumInfo>();
    List<HashMap> producerList = null;
    if (params.getTimeUnit().equals(timeUnit[0])) {
      producerList = analyseMyIbaitsDAO
          .queryForList("monitor_kuroro_producer_analyse_hour.selectKuroroDaySum", params,
              HashMap.class);
      logger.debug("hour..........");
    } else {
      producerList = analyseMyIbaitsDAO
          .queryForList("monitor_kuroro_producer_analyse_day.selectKuroroDaySum", params,
              HashMap.class);
      logger.debug("day...........");
    }
    Map<String, KuroroSumInfo> sums = new HashMap<String, KuroroSumInfo>();
    handInCount(producerList, sums);
    result.addAll(sums.values());
    return result;
  }

  /**
   * 获取所有topic消费量的统计信息
   */
  public List<KuroroSumInfo> consumerDaySum(KuroroParam params) {
    List<KuroroSumInfo> result = new ArrayList<KuroroSumInfo>();
    List<HashMap> consumerList = null;
    if (params.getTimeUnit().equals(timeUnit[0])) {
      consumerList = analyseMyIbaitsDAO
          .queryForList("monitor_kuroro_consumer_analyse_hour.selectKuroroDaySum", params,
              HashMap.class);
    } else {
      consumerList = analyseMyIbaitsDAO
          .queryForList("monitor_kuroro_consumer_analyse_day.selectKuroroDaySum", params,
              HashMap.class);
    }
    Map<String, KuroroSumInfo> sums = new HashMap<String, KuroroSumInfo>();
    handOutCount(consumerList, sums);
    result.addAll(sums.values());
    return result;
  }

  public Set<String> getValues(String key) {
    if (ATOMIC_LONG.incrementAndGet() % 10 == 0) {
      initCombox();
    }
    return combox.get(key);
  }

  public Map<String, Object> parseSetToComboData(String key) {
    Set<String> set = getValues(key);
    Map<String, Object> resultMap = new HashMap<String, Object>();
    if (set != null && set.size() > 0) {
      List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
      for (String str : set) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("value", str);
        map.put("text", str);
        list.add(map);
      }
      resultMap.put("root", list);
    }
    return resultMap;
  }

  public Map<String, Object> getAllZoneName(String key) {
    Set<String> set = KuroroZkUtil.allIdcZoneName();
    Map<String, Object> resultMap = new HashMap<String, Object>();
    if (set != null && set.size() > 0) {
      List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
      for (String str : set) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("value", str);
        map.put("text", str);
        list.add(map);
      }
      resultMap.put("root", list);
    }
    return resultMap;
  }


  @SuppressWarnings("rawtypes")
  public void initCombox() {
    KuroroParam kuroroParam = new KuroroParam();
    Date oneDayAgo = new Date(System.currentTimeMillis() - oneHour * 4);
    kuroroParam.setStartTime(oneDayAgo);
    List<HashMap> list = analyseMyIbaitsDAO
        .queryForList("monitor_kuroro_consumer_analyse_hour.getComboxInfo", kuroroParam,
            HashMap.class);
    kuroroParam.setStartTime(new Date(System.currentTimeMillis() - 15 * oneMins));
    List<HashMap> minsList = analyseMyIbaitsDAO
        .queryForList("monitor_kuroro_consumer_analyse.getComboxInfo", kuroroParam, HashMap.class);
    list.addAll(minsList);
    Set<String> topicList = combox.get(topics);
    if (topicList == null) {
      topicList = new HashSet<String>();
      topicList.addAll(getExistTopics());
      combox.put(topics, topicList);
    }

    for (HashMap curt : list) {
      String topicName = (String) curt.get("TOPIC_NAME");
      String broker = (String) curt.get("BROKER");
      String consumerName = (String) curt.get("CONSUMER_NAME");
      String consumerIp = (String) curt.get("CONSUMER_IP");
      Number connPort = (Number) curt.get("CONN_PORT");
      topicList.add(topicName);
      String brokerKey = topicName + at + brokers;
      Set<String> brokerList = combox.get(topicName + at + brokers);
      if (brokerList == null) {
        brokerList = new HashSet<String>();
        combox.put(brokerKey, brokerList);
      }
      brokerList.add(broker);

      String consumersKey = topicName + at + consumers;
      Set<String> consumerList = combox.get(consumersKey);
      if (consumerList == null) {
        consumerList = new HashSet<String>();
        combox.put(consumersKey, consumerList);
      }
      consumerList.add(consumerName);

      String consumersIpKey = topicName + at + consumers + at + consumerIps;
      Set<String> consumersIpList = combox.get(consumersIpKey);
      if (consumersIpList == null) {
        consumersIpList = new HashSet<String>();
        combox.put(consumersIpKey, consumersIpList);
      }
      consumersIpList.add(consumerIp);

      String connPortsKey = topicName + at + consumers + at + consumerIps + at + connPorts;
      Set<String> connPortsList = combox.get(connPortsKey);
      if (connPortsList == null) {
        connPortsList = new HashSet<String>();
        combox.put(connPortsKey, connPortsList);
      }
      connPortsList.add(connPort + "");
    }

    Set<String> zoneSet = combox.get(zones);
    if (zoneSet == null) {
      zoneSet = MonitorZkUtil.getAllZoneNameInCurrentIdc();
      combox.put(zones, zoneSet);
    }
  }

  public Set<String> getExistTopics() {
    Set<String> existTopics = new HashSet<String>();

    List<String> topics = MonitorZkUtil.getLocalIDCZk().getChildren(MonitorZkUtil.IDCZkTopicPath);
    existTopics.addAll(topics);
    return existTopics;
  }
}

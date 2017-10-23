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

import com.epocharch.common.config.PropertiesContainer;
import com.epocharch.common.util.SystemUtil;
import com.epocharch.kuroro.broker.constants.BrokerInConstants;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.monitor.common.DateUtil;
import com.epocharch.kuroro.monitor.common.ExceptionInfoUtil;
import com.epocharch.kuroro.monitor.dao.impl.BaseMybatisDAOImpl;
import com.epocharch.kuroro.monitor.dto.KuroroConsumerAnalyse;
import com.epocharch.kuroro.monitor.dto.KuroroParam;
import com.epocharch.kuroro.monitor.dto.KuroroProducerAnalyse;
import com.epocharch.kuroro.monitor.dto.TimeInterval;
import com.epocharch.kuroro.monitor.plugin.AnalysePlugin;
import com.epocharch.kuroro.monitor.plugin.AnalyseUtil;
import com.epocharch.kuroro.monitor.plugin.ResultHandler;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author dongheng
 */
public class AnalystJobService implements InitializingBean {

  public static final String kuroro_monitor_servers = MonitorZkUtil.IDCZkMonitorPath;
  public static final String receiverEmail_2 = Constants.PER_SEND_EMAIL_LIST;
  static final Integer oneHour = 60 * 60 * 1000;
  static final Integer oneDay = 24 * 60 * 60 * 1000;
  private static final Logger logger = Logger.getLogger(AnalystJobService.class);
  private static final int oneMins = 60 * 1000;
  private static ScheduledExecutorService scheduledExecutor;
  private static long period = 60000L;
  private static AtomicLong execCounts = new AtomicLong(0);
  private static java.util.SortedSet<String> serverIpSet;
  private static String IP = SystemUtil.getLocalhostIp();
  private static String childPath = MonitorZkUtil.IDCZkMonitorPath + Constants.SEPARATOR + IP;
  public final String receiverEmail_1 = Constants.PER_SEND_EMAIL_LIST;
  private BaseMybatisDAOImpl memoMyIbaitsDAO;
  private  BaseMybatisDAOImpl analyseMyIbaitsDAO;
  private KuroroAnalystService kuroroAnalystService;
  /**
   * key为plugin的className
   */
  private Map<String, ResultHandler> handlerMap;
  private IZkChildListener alwaysOnlineListener = null;

  public static boolean isProduction() {
    return StringUtils.equals("production", PropertiesContainer.getInstance().getProperty(
        BrokerInConstants.KURORO_BORKER_NAMESPACE, InternalPropKey.CURRENT_ENV));
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    init();
    scheduledExecutor = Executors.newScheduledThreadPool(3);
    scheduledExecutor
        .scheduleAtFixedRate(getScheduleBizLogTask(), 60000, period, TimeUnit.MILLISECONDS);
    scheduleHourMergeTask();
    scheduleDayMergeTask();
  }

  public void setMemoMyIbaitsDAO(BaseMybatisDAOImpl memoMyIbaitsDAO) {
    this.memoMyIbaitsDAO = memoMyIbaitsDAO;
  }

  public void setAnalyseMyIbaitsDAO(
      BaseMybatisDAOImpl analyseMyIbaitsDAO) {
    this.analyseMyIbaitsDAO = analyseMyIbaitsDAO;
  }

  /**
   * 获取需定时执行Task， 日志发送规则：每次往队列发送不超过defaultCount
   */
  protected Runnable getScheduleBizLogTask() {
    return new Runnable() {
      public void run() {
        long curtCounts = execCounts.incrementAndGet();
        Date now = new Date();

        //每隔60s，将符合条件的中间结果写入DB
        insertThirdParty(now);
        if (isDeleteMaster()) {
          deleteOldMemoData(now);
        }
        //每隔3分钟，刷新Combox
        if (curtCounts % 3 == 0) {
          refresh(now);

        }

        /** 每半小时，检查到异常状况后发送告警邮件 */
        if (curtCounts % 30 == 0) {
          String content = ExceptionInfoUtil.getNotifyContent();
          if (StringUtils.isNotEmpty(content)) {
          }
        }
      }
    };
  }

  /**
   * 定时刷新Combox
   */
  public void refresh(final Date now) {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        /** 3分钟刷新一次配置数据 */
        try {
          kuroroAnalystService.initCombox();
        } catch (Exception e) {
          ExceptionInfoUtil.putExceptionLogs("kuroroAnalystService initCombox error", e);
          logger.error("kuroroAnalystService initCombox error", e);
        }
      }
    };
    Thread thread = new Thread(task);
    thread.setName("refresh and clear Combox thread");
    thread.start();
  }

  public void insertThirdParty(Date now) {
    String pluginName = "";
    try {
      for (Map.Entry<String, ResultHandler> entry : handlerMap.entrySet()) {
        pluginName = entry.getKey();
        Map<String, AnalysePlugin> map = AnalyseUtil.getAnalyseMap()
            .get(Class.forName(entry.getKey()));
        if (map != null && map.size() > 0) {
          Iterator<String> itr = map.keySet().iterator();
          while (itr.hasNext()) {
            String key = itr.next();
            TimeInterval timeInterval = null;
            try {
              AnalysePlugin analysePlugin = map.get(key);
              timeInterval = analysePlugin.getTimeInterval();
            } catch (Exception e) {
              logger.error("insertThirdParty :" + key, e);
              throw new Exception("map.get(key).getTimeInterval() is error");
            }
            Date intervalAgo = new Date(now.getTime() - 23000);
            String nowIn = DateUtil
                .getMinsStartString(TimeInterval.getStartTime(now, timeInterval));
            String agoIn = DateUtil
                .getMinsStartString(TimeInterval.getStartTime(intervalAgo, timeInterval));
            if (!StringUtils.equals(key, nowIn) && !StringUtils.equals(key, agoIn)) {
              try {
                entry.getValue().handle(map.get(key).toEntity());
              } catch (Exception e) {
                ExceptionInfoUtil
                    .putExceptionLogs("insertThirdParty error,plugin name:" + pluginName, e);
                logger.error("insertThirdParty", e);
              } finally {
                itr.remove();
              }
            }
          }
        }
      }
    } catch (Exception e) {
      ExceptionInfoUtil.putExceptionLogs("insertThirdParty error,plugin name:" + pluginName, e);
      logger.error("handler error,plugin name:" + pluginName, e);
    }
  }

  public Map<String, ResultHandler> getHandlerMap() {
    return handlerMap;
  }

  public void setHandlerMap(Map<String, ResultHandler> handlerMap) {
    this.handlerMap = handlerMap;
  }

  public void mergeKuroroHourData() {
    if (!isMergeMaster()) {
      logger.info(">>>>>>>>>>>>> current server is not MergeMaster, so hour merge job end.");
      return;
    }

    KuroroParam params = new KuroroParam();
    String[] ymdhms = DateUtil.getCurrentDateTime().split(" ");
    String[] hms = ymdhms[1].split(":");
    String newDate = ymdhms[0] + " " + hms[0] + ":" + "00:" + "00";
    Date end = DateUtil.getDate(newDate, DateUtil.DateTimeFormatString);
    Date start = new Date(end.getTime() - oneHour);
    params.setStartTime(start);
    params.setEndTime(end);
    try {
      // 1 按topic查询出需要做汇总的topic
      List<String> topics = getTopics();
      logger.info(">>>>>>>>>>>>> mergeMessageHourData start : topicName count=" + topics.size()
          + ", start : " + DateUtil
          .getFormatTime(start) + ", end : " + DateUtil.getFormatTime(end));
      for (String topicName : topics) {
        // 2 检查当前topic数据是否汇总过
        params.setTopicName(topicName);

        if (!checkProducerHourMerged(params)) {

          mergeProducerData(params);
        }

        if (!checkConsumerHourMerged(params)) {
          mergeConsumerData(params);
        }
      }
      logger.info(">>>>>>>>>>>>> mergeKuroroHourData end");
    } catch (Exception e) {
      logger.error(">>>>>>>>>>>>> merge kuroro data error.", e);
    }
  }

  /**
   * 判断Kuroro当前统计小时的数据是否合并过
   */
  public boolean checkProducerHourMerged(KuroroParam params) {
    int counts = (Integer) analyseMyIbaitsDAO
        .queryForObject("monitor_kuroro_producer_analyse_hour.checkMerged", params);
    return (counts > 0);
  }

  /**
   * 判断Kuroro当前统计小时的数据是否合并过
   */
  public boolean checkConsumerHourMerged(KuroroParam params) {
    int counts = (Integer) analyseMyIbaitsDAO
        .queryForObject("monitor_kuroro_consumer_analyse_hour.checkMerged", params);
    return (counts > 0);
  }

  /**
   * 合并Kuroro统计数据
   */
  @SuppressWarnings("unchecked")
  private boolean mergeProducerData(KuroroParam params) {
    List<KuroroProducerAnalyse> producerList = null;
    try {
      producerList = analyseMyIbaitsDAO
          .queryForList("monitor_kuroro_producer_analyse.fetchMergeData", params);
    } catch (Exception e) {
      logger.error(
          "monitor_kuroro_producer_analyse.fetchMergeData is error,topic:" + params.getTopicName(),
          e);
    }
    Map<String, KuroroProducerAnalyse> producerMap = new HashMap<String, KuroroProducerAnalyse>();
    if (producerList != null) {
      for (KuroroProducerAnalyse producerAnalyse : producerList) {
        String key = builderKey(producerAnalyse);
        KuroroProducerAnalyse mergeAnalyse = producerMap.get(key);
        if (mergeAnalyse == null) {
          producerAnalyse.setStartTime(params.getStartTime());
          producerAnalyse.setEndTime(params.getEndTime());
          producerMap.put(key, producerAnalyse);
        } else {
          mergeAnalyse.merge(producerAnalyse);
        }
      }
    }
    if (producerMap.size() > 0) {
      List<KuroroProducerAnalyse> insertList = new ArrayList<KuroroProducerAnalyse>();
      insertList.addAll(producerMap.values());
      for (KuroroProducerAnalyse curt : insertList) {
        curt.setId(null);
      }
      try {
        analyseMyIbaitsDAO.batchInsert("monitor_kuroro_producer_analyse_hour.insert", insertList);
      } catch (Exception r) {
        logger.error("insert into monitor_kuroro_producer_analyse_hour error", r);
        return false;
      }
    }

    return true;
  }

  /**
   * 合并Mq统计数据
   */
  @SuppressWarnings("unchecked")
  private boolean mergeConsumerData(KuroroParam params) {
    List<KuroroConsumerAnalyse> list = null;
    try {
      list = analyseMyIbaitsDAO
          .queryForList("monitor_kuroro_consumer_analyse.fetchMergeData", params);
    } catch (Exception e) {
      logger.error(
          "queryForList :kuroro consumer fetchMergeData is error topic:" + params.getTopicName(),
          e);
    }

    Map<String, KuroroConsumerAnalyse> consumerMap = new HashMap<String, KuroroConsumerAnalyse>();
    if (list != null) {
      for (KuroroConsumerAnalyse consumerAnalyse : list) {
        String key = builderKey(consumerAnalyse);
        KuroroConsumerAnalyse mergeAnalyse = consumerMap.get(key);
        if (mergeAnalyse == null) {
          consumerAnalyse.setStartTime(params.getStartTime());
          consumerAnalyse.setEndTime(params.getEndTime());
          consumerMap.put(key, consumerAnalyse);
        } else {
          mergeAnalyse.merge(consumerAnalyse);
        }
      }
    }
    if (consumerMap.size() > 0) {
      List<KuroroConsumerAnalyse> insertList = new ArrayList<KuroroConsumerAnalyse>();
      insertList.addAll(consumerMap.values());
      for (KuroroConsumerAnalyse curt : insertList) {
        curt.setId(null);
      }

      try {
        analyseMyIbaitsDAO.batchInsert("monitor_kuroro_consumer_analyse_hour.insert", insertList);
      } catch (Exception e) {
        logger.error("insert into monitor_kuroro_consumer_analyse_hour error ", e);
        return false;
      }
    }

    return true;
  }

  /*
   * 合并Kuroro每天的数据,以天为单位统计topic数据
   * */
  public void mergeKuroroDayData() {
    if (!isMergeMaster()) {
      logger.info(">>>>>>>>>>>>> current server is not MergeMaster, so day merge job end.");
      return;
    }

    KuroroParam params = new KuroroParam();
    String[] ymdhms = DateUtil.getCurrentDateTime().split(" ");
    String newDate = ymdhms[0] + " " + "00:" + "00:" + "00";
    Date end = DateUtil.getDate(newDate, DateUtil.DateTimeFormatString);
    Date start = new Date(end.getTime() - oneDay);
    params.setStartTime(start);
    params.setEndTime(end);
    try {
      // 1 按topic查询出需要做汇总的topic
      List<String> topics = getTopics();
      logger.info(
          ">>>>>>>>>>>>> mergeKuroroDayData start : topicName count=" + topics.size() + ", start : "
              + DateUtil
              .getFormatTime(start) + ", end : " + DateUtil.getFormatTime(end));
      if (!checkProducerDayMerged(params)) {
        mergeProducerDayData(params);
      }
      if (!checkConsumerDayMerged(params)) {
        mergeConsumerDayData(params);
      }
      logger.info(">>>>>>>>>>>>> mergeKuroroDayData end");
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(">>>>>>>>>>>>> merge Kuroro day data error.", e);
    }
  }

  /**
   * 合并kuroro 每天生产的统计数据
   */
  @SuppressWarnings("unchecked")
  private boolean mergeProducerDayData(KuroroParam params) {
    List<KuroroProducerAnalyse> producerList = null;
    try {
      producerList = analyseMyIbaitsDAO
          .queryForList("monitor_kuroro_producer_analyse_hour.fetchMergeData", params);
    } catch (Exception e) {
      e.printStackTrace();
    }
    Map<String, KuroroProducerAnalyse> producerMap = new HashMap<String, KuroroProducerAnalyse>();
    if (producerList != null) {
      for (KuroroProducerAnalyse producerAnalyse : producerList) {
        String key = builderKey(producerAnalyse);
        KuroroProducerAnalyse mergeAnalyse = producerMap.get(key);
        if (mergeAnalyse == null) {
          producerAnalyse.setStartTime(params.getStartTime());
          producerAnalyse.setEndTime(params.getEndTime());
          producerMap.put(key, producerAnalyse);
        } else {
          mergeAnalyse.merge(producerAnalyse);
        }
      }
    }
    if (producerMap.size() > 0) {
      List<KuroroProducerAnalyse> insertList = new ArrayList<KuroroProducerAnalyse>();
      insertList.addAll(producerMap.values());
      for (KuroroProducerAnalyse curt : insertList) {
        curt.setId(null);
      }
      try {
        analyseMyIbaitsDAO.batchInsert("monitor_kuroro_producer_analyse_day.insert", insertList);
      } catch (Exception r) {
        r.printStackTrace();
        logger.error("insert into monitor_kuroro_producer_analyse_day error", r);
        return false;
      }
    }

    return true;
  }

  /**
   * 合并Kuroro每天消费的统计数据
   */
  @SuppressWarnings("unchecked")
  private boolean mergeConsumerDayData(KuroroParam params) {
    List<KuroroConsumerAnalyse> list = analyseMyIbaitsDAO
        .queryForList("monitor_kuroro_consumer_analyse_hour.fetchMergeData", params);
    logger.warn("merge :Kuroro consumer fetchMergeData.size=" + list.size());
    Map<String, KuroroConsumerAnalyse> consumerMap = new HashMap<String, KuroroConsumerAnalyse>();
    if (list != null) {
      for (KuroroConsumerAnalyse consumerAnalyse : list) {
        String key = builderKey(consumerAnalyse);
        KuroroConsumerAnalyse mergeAnalyse = consumerMap.get(key);
        if (mergeAnalyse == null) {
          consumerAnalyse.setStartTime(params.getStartTime());
          consumerAnalyse.setEndTime(params.getEndTime());
          consumerMap.put(key, consumerAnalyse);
        } else {
          mergeAnalyse.merge(consumerAnalyse);
        }
      }
    }
    if (consumerMap.size() > 0) {
      List<KuroroConsumerAnalyse> insertList = new ArrayList<KuroroConsumerAnalyse>();
      insertList.addAll(consumerMap.values());
      for (KuroroConsumerAnalyse curt : insertList) {
        curt.setId(null);
      }

      try {
        analyseMyIbaitsDAO.batchInsert("monitor_kuroro_consumer_analyse_day.insert", insertList);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("insert into monitor_kuroro_consumer_analyse_day error ", e);
        return false;
      }
    }

    return true;
  }

  /**
   * 判断Kuroro当天的生产数据是否合并过
   */
  public boolean checkProducerDayMerged(KuroroParam params) {
    int counts = (Integer) analyseMyIbaitsDAO
        .queryForObject("monitor_kuroro_producer_analyse_day.checkMerged", params);
    return (counts > 0);
  }

  /**
   * 判断Kuroro当天的消费数据是否合并过
   */
  public boolean checkConsumerDayMerged(KuroroParam params) {
    int counts = (Integer) analyseMyIbaitsDAO
        .queryForObject("monitor_kuroro_consumer_analyse_day.checkMerged", params);
    return (counts > 0);
  }

  private String builderKey(KuroroProducerAnalyse kuroroProducerAnalyse) {
    return new StringBuilder().append(kuroroProducerAnalyse.getTopicName())
        .append(kuroroProducerAnalyse.getBroker()).toString();
  }

  private String builderKey(KuroroConsumerAnalyse kuroroConsumerAnalyse) {
    return new StringBuilder().append(kuroroConsumerAnalyse.getTopicName())
        .append(kuroroConsumerAnalyse.getBroker())
        .append(kuroroConsumerAnalyse.getConsumerName()).append(kuroroConsumerAnalyse.getConsumerIp())
        .append(kuroroConsumerAnalyse.getConnPort())
        .toString();
  }

  /**
   * 获取需要做小时数据合并的TopicName列表
   */
  private List<String> getTopics() {
    List<String> list = new ArrayList<String>();
    try {
      list.addAll(kuroroAnalystService.getExistTopics());
    } catch (Exception e) {
      logger.error("get tops form mastet error.", e);
    }
    return list;
  }

  /**
   * 删除内存统计的老数据
   */
  public void deleteOldMemoData(Date now) {
    Date param = new Date(now.getTime() - 18 * oneMins);
    try {
      memoMyIbaitsDAO.delete("monitor_kuroro_producer_analyse.deleteOld", param);
      memoMyIbaitsDAO.delete("monitor_kuroro_consumer_analyse.deleteOld", param);
    } catch (Exception e) {
      logger.error("delete old memory date error.", e);
    }
  }

  public void setKuroroAnalystService(KuroroAnalystService kuroroAnalystService) {
    this.kuroroAnalystService = kuroroAnalystService;
  }

  //每小时执行一次的定时任务
  private void scheduleHourMergeTask() {
    long initialDelay = 1;
    long period = 1;
    scheduledExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        mergeKuroroHourData();
      }
    }, initialDelay, period, TimeUnit.HOURS);
  }

  //每天凌晨3点执行一次的定时任务
  private void scheduleDayMergeTask() {
    long initialDelay;
    long period = 24;
    java.util.Calendar calendar = java.util.Calendar.getInstance();
    int hourOfDay = calendar.get(java.util.Calendar.HOUR_OF_DAY);
    if (hourOfDay > 3) {
      initialDelay = (24 - hourOfDay) + 3;
    } else {
      initialDelay = 3 - hourOfDay;
    }
    scheduledExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        mergeKuroroDayData();
      }
    }, initialDelay, period, TimeUnit.HOURS);
  }

  private void init() {
    final ZkClient _zkClient = MonitorZkUtil.getLocalIDCZk();
    if (!_zkClient.exists(MonitorZkUtil.IDCZkMonitorPath)) {
      _zkClient.createPersistent(MonitorZkUtil.IDCZkMonitorPath, true);
    }
    logger.info("Initialization AnalystJobService, Kuroro-monitor is zk path for "
        + MonitorZkUtil.IDCZkMonitorPath + "!");
    if (_zkClient.exists(childPath)) {
      _zkClient.delete(childPath);
    }
    if (!_zkClient.exists(childPath)) {
      _zkClient.createEphemeral(childPath, IP);
      logger.info("Kuroro-monitor server register successfully to zk, path is : " + childPath);
    }
    alwaysOnlineListener = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds)
          throws Exception {
        if (currentChilds == null || !currentChilds.contains(IP)) {
          _zkClient.createEphemeral(childPath, IP);
        }
        setServerIpSet(currentChilds);
      }
    };
    _zkClient.subscribeChildChanges(kuroro_monitor_servers, alwaysOnlineListener);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        logger.info("Start to Stop Kuroro-monitor Server......");
        //                try {
        ZkClient _zkClient = MonitorZkUtil.getLocalIDCZk();
        if (alwaysOnlineListener != null) {
          _zkClient.unsubscribeChildChanges(kuroro_monitor_servers, alwaysOnlineListener);
        }
        if (_zkClient.exists(childPath)) {
          _zkClient.delete(childPath);
        }
        logger.info("Kuroro-monitor Service is successfully stoped.");
      }
    });

    List<String> ipList = MonitorZkUtil.getLocalIDCZk().getChildren(kuroro_monitor_servers);
    setServerIpSet(ipList);
  }

  private void setServerIpSet(List<String> ipList) {
    if (ipList == null) {
      ipList = new ArrayList<String>();
    }
    serverIpSet = new java.util.concurrent.ConcurrentSkipListSet<String>(ipList);
    logger.info("ServerIpSet is changed, ConcurrentSkipListSet is : " + ipList);
  }

  private boolean isMergeMaster() {
    return serverIpSet != null && IP.equals(serverIpSet.first());
  }

  private boolean isDeleteMaster() {
    return serverIpSet != null && IP.equals(serverIpSet.last());
  }
}

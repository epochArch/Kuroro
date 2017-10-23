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

package com.epocharch.kuroro.monitor.dto;

import com.epocharch.kuroro.common.message.MessageLog;
import com.epocharch.kuroro.common.message.OutMessagePair;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dongheng
 */
public class KuroroStatistics {

  private String topicName;
  private String zoneName;
  private AtomicLong in = new AtomicLong(0);
  private AtomicLong out = new AtomicLong(0);
  /**
   * key为broker IP
   */
  private Map<String, AtomicLong> brokerIn = new ConcurrentHashMap<String, AtomicLong>();
  /**
   * 外层key为broker IP ,内层key为coumserName
   */
  private Map<String, Map<String, CoumserOutInfo>> brokerOut = new ConcurrentHashMap<String, Map<String, CoumserOutInfo>>();

  public void statistics(MessageLog messageLog) {
    if (messageLog.getIn() != null && messageLog.getIn() > 0) {
      in.addAndGet(messageLog.getIn());
      AtomicLong brIn = brokerIn.get(messageLog.getBroker());
      if (brIn == null) {
        brIn = new AtomicLong(0);
        brokerIn.put(messageLog.getBroker(), brIn);
      }
      brIn.addAndGet(messageLog.getIn());
    }
    if (messageLog.getOut() != null) {
      for (OutMessagePair outInfo : messageLog.getOut()) {
        if (outInfo.getCount() > 0) {
          out.addAndGet(outInfo.getCount());
          Map<String, CoumserOutInfo> brOut = brokerOut.get(messageLog.getBroker());
          String consumerName = messageLog.getConsumerName();
          if (brOut == null) {
            brOut = new ConcurrentHashMap<String, CoumserOutInfo>();
            CoumserOutInfo coumserOutInfo = new CoumserOutInfo();
            coumserOutInfo.setConsumerName(consumerName);
            coumserOutInfo.addReceiveCounts(outInfo.getConsumerIP(), outInfo.getCount());
            brOut.put(consumerName, coumserOutInfo);
            brokerOut.put(messageLog.getBroker(), brOut);
          } else {
            CoumserOutInfo coumserOutInfo = brOut.get(consumerName);
            if (coumserOutInfo == null) {
              coumserOutInfo = new CoumserOutInfo();
              coumserOutInfo.setConsumerName(consumerName);
              brOut.put(consumerName, coumserOutInfo);
            }
            coumserOutInfo.addReceiveCounts(outInfo.getConsumerIP(), outInfo.getCount());
          }
        }
      }
    }

  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public AtomicLong getIn() {
    return in;
  }

  public void setIn(AtomicLong in) {
    this.in = in;
  }

  public AtomicLong getOut() {
    return out;
  }

  public void setOut(AtomicLong out) {
    this.out = out;
  }

  public Map<String, AtomicLong> getBrokerIn() {
    return brokerIn;
  }

  public void setBrokerIn(Map<String, AtomicLong> brokerIn) {
    this.brokerIn = brokerIn;
  }

  public Map<String, Map<String, CoumserOutInfo>> getBrokerOut() {
    return brokerOut;
  }

  public void setBrokerOut(Map<String, Map<String, CoumserOutInfo>> brokerOut) {
    this.brokerOut = brokerOut;
  }

  public String getZoneName() {
    return zoneName;
  }

  public void setZoneName(String zoneName) {
    this.zoneName = zoneName;
  }

  public KuroroInOutAnalyse toEntity() {
    KuroroInOutAnalyse kuroroInOutAnalyse = new KuroroInOutAnalyse();
    /** 生成kuroroProducerAnalyse */
    for (Map.Entry<String, AtomicLong> inEntry : brokerIn.entrySet()) {
      KuroroProducerAnalyse kuroroProducerAnalyse = new KuroroProducerAnalyse();
      kuroroProducerAnalyse.setBroker(inEntry.getKey());
      kuroroProducerAnalyse.setInCounts(inEntry.getValue().intValue());
      kuroroProducerAnalyse.setTopicName(topicName);
      kuroroProducerAnalyse.setZone(zoneName);
      kuroroInOutAnalyse.addProducerAnalyse(kuroroProducerAnalyse);
    }

    /** 生成consumerAnalyses */
    for (Entry<String, Map<String, CoumserOutInfo>> brOutEntry : brokerOut.entrySet()) {
      for (Entry<String, CoumserOutInfo> consureEntry : brOutEntry.getValue().entrySet()) {
        CoumserOutInfo coumserOutInfo = consureEntry.getValue();
        for (Map.Entry<String, AtomicLong> entry : coumserOutInfo.getConsumerHosts().entrySet()) {
          KuroroConsumerAnalyse kuroroConsumerAnalyse = new KuroroConsumerAnalyse();
          kuroroConsumerAnalyse.setConsumerName(consureEntry.getKey());
          kuroroConsumerAnalyse.setBroker(brOutEntry.getKey());
          kuroroConsumerAnalyse.setConsumerIp(entry.getKey());
          kuroroConsumerAnalyse.setOutCounts((int) entry.getValue().get());
          kuroroConsumerAnalyse.setTopicName(topicName);
          kuroroConsumerAnalyse.setZone(zoneName);
          kuroroInOutAnalyse.addConsumerAnalyse(kuroroConsumerAnalyse);
        }
      }
    }

    return kuroroInOutAnalyse;
  }
}

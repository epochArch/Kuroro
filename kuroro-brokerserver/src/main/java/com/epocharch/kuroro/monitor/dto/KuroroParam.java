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

import java.io.Serializable;
import java.util.Date;

/**
 * 消息报表查询参数
 *
 * @author dongheng
 */
public class KuroroParam implements Serializable {

  private static final long serialVersionUID = -4711756812536579069L;
  private String topicName;
  private String broker;
  private String consumerIp;
  private Integer connPort;
  private String consumerName;
  private Date startTime;
  private Date endTime;
  private String timeUnit;
  private String zone;

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public String getBroker() {
    return broker;
  }

  public void setBroker(String broker) {
    this.broker = broker;
  }

  public String getConsumerIp() {
    return consumerIp;
  }

  public void setConsumerIp(String consumerIp) {
    this.consumerIp = consumerIp;
  }

  public Integer getConnPort() {
    return connPort;
  }

  public void setConnPort(Integer connPort) {
    this.connPort = connPort;
  }

  public String getConsumerName() {
    return consumerName;
  }

  public void setConsumerName(String consumerName) {
    this.consumerName = consumerName;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(String timeUnit) {
    this.timeUnit = timeUnit;
  }

  public String getZone() {
    return zone;
  }

  public void setZone(String zone) {
    this.zone = zone;
  }
}

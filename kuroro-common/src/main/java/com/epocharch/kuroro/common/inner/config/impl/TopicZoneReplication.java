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

package com.epocharch.kuroro.common.inner.config.impl;

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import java.io.Serializable;

/**
 * @author pengrongxin 2014年8月25日 上午10:55:38 Topic跨IDC消费指标
 */
public class TopicZoneReplication implements Serializable {

  private static final long serialVersionUID = 2948481279634515243L;

  private String topic;

  private String destTopic;

  /**
   * Transfer 为指定的topic创建的consumer的id,如果和其他地方的consumer 的id相同，
   * 则是竞争消费, 否则是独立消费
   */
  private String consumerId;

  // transfer 对指定的topic的消费速度 1000 代表每秒1000个消息
  private int consumerSpeed;
  // 消息过滤属性，用‘|’隔开
  private String filter;

  private boolean activeFlag = true;

  private boolean sync;

  // topic 传输的目的地
  private String destZone;
  // topic 发源地
  private String srcZone;

  private String group;

  private int speedPercent;

  // topic 传输的目的idc
  private String destIdc;

  // topic 发源idc
  private String srcIdc;

  public String getDestTopic() {
    return destTopic;
  }

  public void setDestTopic(String destTopic) {
    this.destTopic = destTopic;
  }

  public int getConsumerSpeed() {
    return consumerSpeed;
  }

  public void setConsumerSpeed(int consumerSpeed) {
    this.consumerSpeed = consumerSpeed;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public boolean isActiveFlag() {
    return activeFlag;
  }

  public void setActiveFlag(boolean activeFlag) {
    this.activeFlag = activeFlag;
  }

  public String getDestZone() {
    return destZone;
  }

  public void setDestZone(String destZone) {
    this.destZone = destZone;
  }

  public String getSrcZone() {
    return srcZone;
  }

  public void setSrcZone(String srcZone) {
    this.srcZone = srcZone;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public boolean isSync() {
    return sync;
  }

  public void setSync(boolean sync) {
    this.sync = sync;
  }

  public int getSpeedPercent() {
    return speedPercent;
  }

  public void setSeedPercent(int speedPercent) {
    this.speedPercent = speedPercent;
  }

  @Override
  public boolean equals(Object topicRep) {
    if (!(topicRep instanceof TopicZoneReplication)) {
      return false;
    }
    TopicZoneReplication other = (TopicZoneReplication) topicRep;
    if (other.getDestTopic().equals(this.destTopic) && other.getConsumerId()
        .equals(this.consumerId)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    StringBuilder sb = new StringBuilder(this.destTopic);
    sb.append(consumerId);
    return sb.toString().hashCode();
  }

  @Override
  public String toString() {
    return topic + "[zoneName=" + destZone + ", idcName=" + this.destIdc + ", consumerId="
        + this.consumerId + ", destTopic="
        + this.destTopic + ", speed=" + this.consumerSpeed + ", speedPercent=" + this.speedPercent
        + ", messageFilter=" + filter
        + ", isAsync=" + sync + "]";
  }

  public String getTopicRepKey() {
    return this.consumerId + this.destTopic;
  }

  public String getZKNodePath() {
    StringBuilder path = null;
    if (!KuroroUtil.isBlankString(this.destZone)) {
      path = new StringBuilder(this.destZone);
    } else {
      path = new StringBuilder(this.destIdc);
    }
    path.append("#").append(this.consumerId).append("#").append(this.destTopic);
    return path.toString();
  }

  public String getGroup() {
    if (group == null || group.isEmpty()) {
      return Constants.KURORO_DEFAULT_BROKER_GROUPNAME;
    }
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public String getDestIdc() {
    return destIdc;
  }

  public void setDestIdc(String destIdc) {
    this.destIdc = destIdc;
  }

  public String getSrcIdc() {
    return srcIdc;
  }

  public void setSrcIdc(String srcIdc) {
    this.srcIdc = srcIdc;
  }
}

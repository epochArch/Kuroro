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

package com.epocharch.kuroro.broker.leaderserver;

import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import java.io.Serializable;

public class WrapTopicConsumerIndex implements Serializable {

  public static final String CMD_COMPENSATION = "cps";
  public static final String CMD_HEARTBEAT = "hb";
  public static final WrapTopicConsumerIndex HEARTBEAT = new WrapTopicConsumerIndex();
  private static final long serialVersionUID = -2192338145524731691L;

  static {
    HEARTBEAT.setCommand(CMD_HEARTBEAT);
  }

  private String topicName;
  private String consumerId;
  private Integer index;
  private Long squence;
  private String command;
  private String zone;
  private ConsumerOffset consumerOffset;
  private transient Long requestTime;

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public Integer getIndex() {
    return index;
  }

  public void setIndex(Integer index) {
    this.index = index;
  }

  public Long getSquence() {
    return squence;
  }

  public void setSquence(Long squence) {
    this.squence = squence;
  }

  public String getTopicConsumerIndex() {
    return zone == null ? topicName + "#" + consumerId + "#" + index : getZoneTopicConsumerIndex();
  }

  public String getConsumerIndex() {
    return consumerId + "#" + index;
  }

  public Long getRequestTime() {
    return requestTime;
  }

  public void setRequestTime(Long requestTime) {
    this.requestTime = requestTime;
  }

  public String getZone() {
    return zone;
  }

  public void setZone(String zone) {
    this.zone = zone;
  }

  public String getZoneTopicConsumerIndex() {
    return topicName + "#" + consumerId + "#" + index + "#" + zone;
  }

  public String getTopicZone() {
    return zone == null ? topicName : topicName + "#" + zone;
  }

  public ConsumerOffset getOffset() {
    return consumerOffset;
  }

  public void setOffset(ConsumerOffset offset) {
    this.consumerOffset = offset;
  }

  @Override
  public String toString() {
    return "WrapTopicConsumerIndex[" + "topicName='" + topicName + '\'' + ", consumerId='"
        + consumerId + '\'' + ", index=" + index
        + ", squence=" + squence + ", command='" + command + '\'' + ", requestTime=" + requestTime
        + ", zone=" + zone + ", offset="
        + consumerOffset + ']';
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (getClass() != o.getClass()) {
      return false;
    }

    if (!(o instanceof WrapTopicConsumerIndex)) {
      return false;
    }

    WrapTopicConsumerIndex that = (WrapTopicConsumerIndex) o;

    if (topicName != null ? !topicName.equals(that.topicName) : that.topicName != null) {
      return false;
    }

    if (consumerId != null ? !consumerId.equals(that.consumerId) : that.consumerId != null) {
      return false;
    }

    if (index != null ? !index.equals(that.index) : that.index != null) {
      return false;
    }

    if (zone != null ? !zone.equals(that.zone) : that.zone != null) {
      return false;
    }

    if (consumerOffset == null) {
      if (that.consumerOffset != null) {
        return false;
      }
    } else if (!consumerOffset.getType().equals(that.consumerOffset.getType())) {
      return false;
    }

    if (consumerOffset != null && that.consumerOffset != null) {
      if (consumerOffset.getOffsetValue() == null) {
        if (that.consumerOffset.getOffsetValue() != null) {
          return false;
        }
      } else if (!consumerOffset.getOffsetValue().equals(that.consumerOffset.getOffsetValue())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = topicName != null ? topicName.hashCode() : 0;
    result = 31 * result + (consumerId != null ? consumerId.hashCode() : 0);
    result = 31 * result + (index != null ? index.hashCode() : 0);
    result = 31 * result + (zone != null ? zone.hashCode() : 0);
    result = 31 * result + (consumerOffset != null ? consumerOffset.getType().hashCode() : 0);
    result =
        31 * result + (((consumerOffset != null) && (consumerOffset.getOffsetValue() != null)) ?
            consumerOffset.getOffsetValue().hashCode() :
            0);
    return result;
  }
}

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

import java.io.Serializable;

public class MessageIDPair implements Serializable {

  public static final String COMPENSATION = "cps";
  public static final String CMD_HEARTBEAT = "hb";
  public static final MessageIDPair HEARTBEAT = new MessageIDPair();
  private static final long serialVersionUID = -5398751919212039064L;

  static {
    HEARTBEAT.setCommand(CMD_HEARTBEAT);
  }

  private Long minMessageId;
  private Long maxMessageId;
  private Long sequence;
  private String command;
  private Boolean isRestOffset = false;
  private String topicConsumerIndex;

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public Long getSequence() {
    return sequence;
  }

  public void setSequence(Long sequence) {
    this.sequence = sequence;
  }

  public String getTopicConsumerIndex() {
    return topicConsumerIndex;
  }

  public void setTopicConsumerIndex(String topicConsumerIndex) {
    this.topicConsumerIndex = topicConsumerIndex;
  }

  public Long getMinMessageId() {
    return minMessageId;
  }

  public void setMinMessageId(Long minMessageId) {
    this.minMessageId = minMessageId;
  }

  public Long getMaxMessageId() {
    return maxMessageId;
  }

  public void setMaxMessageId(Long maxMessageId) {
    this.maxMessageId = maxMessageId;
  }

  public Boolean getRestOffset() {
    return isRestOffset;
  }

  public void setRestOffset(Boolean restOffset) {
    isRestOffset = restOffset;
  }

  @Override
  public String toString() {
    return "MessageIDPair{" + "minMessageId=" + minMessageId + ", maxMessageId=" + maxMessageId
        + ", sequence=" + sequence
        + ", command='" + command + '\'' + ", isRestOffset=" + isRestOffset
        + ", topicConsumerIndex='" + topicConsumerIndex + '\''
        + '}';
  }

}

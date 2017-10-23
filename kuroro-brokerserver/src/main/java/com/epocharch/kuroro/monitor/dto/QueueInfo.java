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

public class QueueInfo {

  private String topicName;
  private Long enQueueCount;
  private String consumerId;
  private String messageFilter;
  private Long deQueueCount;

  public QueueInfo() {
    this.deQueueCount = 0L;
    this.enQueueCount = 0L;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public Long getEnQueueCount() {
    return enQueueCount;
  }

  public void setEnQueueCount(Long enQueueCount) {
    this.enQueueCount = enQueueCount;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public Long getDeQueueCount() {
    return deQueueCount;
  }

  public void setDeQueueCount(Long deQueueCount) {
    this.deQueueCount = deQueueCount;
  }

  public String getMessageFilter() {
    return messageFilter;
  }

  public void setMessageFilter(String messageFilter) {
    this.messageFilter = messageFilter;
  }
}

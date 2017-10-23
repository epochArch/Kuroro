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

package com.epocharch.kuroro.common.inner.wrap;

import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.ConsumerType;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.consumer.ConsumerMessageType;
import com.epocharch.kuroro.common.message.Destination;

public final class WrappedConsumerMessage extends Wrap {

  private static final long serialVersionUID = -5700217400205746693L;
  public static WrappedConsumerMessage HEARTBEAT = new WrappedConsumerMessage(
      ConsumerMessageType.HEARTBEAT);
  private ConsumerMessageType type;
  private Destination dest;
  private String consumerId;
  private Long messageId;
  private ConsumerType consumerType;
  private Boolean needClose;
  private Integer threadCount;
  private MessageFilter messageFilter;
  private String idcName;
  private String zone;
  private String poolId;
  private MessageFilter consumeLocalZoneFilter;
  private ConsumerOffset consumerOffset;
  private Boolean isRestOffset;

  public WrappedConsumerMessage() {
  }

  public WrappedConsumerMessage(ConsumerMessageType type) {
    this.type = type;
  }

  public WrappedConsumerMessage(ConsumerMessageType type, String consumerId, Destination dest,
      ConsumerType consumerType, int threadCount,
      MessageFilter messageFilter) {
    setWrappedType(WrappedType.CONSUMER_SPREAD);
    this.type = type;
    this.dest = dest;
    this.consumerId = consumerId;
    this.consumerType = consumerType;
    this.threadCount = Integer.valueOf(threadCount);
    this.messageFilter = messageFilter;
  }

  public WrappedConsumerMessage(ConsumerMessageType type, Long messageId, boolean needClose) {
    setWrappedType(WrappedType.CONSUMER_ACK);
    this.type = type;
    this.messageId = messageId;
    this.needClose = Boolean.valueOf(needClose);
  }

  public WrappedConsumerMessage(ConsumerMessageType type, Long messageId, boolean needClose,
      Boolean isRestOffset) {
    setWrappedType(WrappedType.CONSUMER_ACK);
    this.type = type;
    this.messageId = messageId;
    this.needClose = Boolean.valueOf(needClose);
    this.isRestOffset = isRestOffset;
  }

  public WrappedConsumerMessage(ConsumerMessageType type, String consumerId, Destination dest,
      ConsumerType consumerType, int threadCount,
      MessageFilter messageFilter, String idcName, String zone, String poolId,
      MessageFilter consumeLocalZoneFilter) {
    setWrappedType(WrappedType.CONSUMER_SPREAD);
    this.type = type;
    this.dest = dest;
    this.consumerId = consumerId;
    this.consumerType = consumerType;
    this.threadCount = Integer.valueOf(threadCount);
    this.messageFilter = messageFilter;
    this.idcName = idcName;
    this.zone = zone;
    this.poolId = poolId;
    this.consumeLocalZoneFilter = consumeLocalZoneFilter;
  }

  public WrappedConsumerMessage(ConsumerMessageType type, String consumerId, Destination dest,
      ConsumerType consumerType, int threadCount,
      MessageFilter messageFilter, String idcName, String zone, String poolId,
      MessageFilter consumeLocalZoneFilter,
      ConsumerOffset consumerOffset) {
    setWrappedType(WrappedType.CONSUMER_SPREAD);
    this.type = type;
    this.dest = dest;
    this.consumerId = consumerId;
    this.consumerType = consumerType;
    this.threadCount = Integer.valueOf(threadCount);
    this.messageFilter = messageFilter;
    this.idcName = idcName;
    this.zone = zone;
    this.poolId = poolId;
    this.consumeLocalZoneFilter = consumeLocalZoneFilter;
    this.consumerOffset = consumerOffset;
  }

  public ConsumerMessageType getType() {
    return type;
  }

  public void setType(ConsumerMessageType type) {
    this.type = type;
  }

  public Destination getDest() {
    return dest;
  }

  public void setDest(Destination dest) {
    this.dest = dest;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public Long getMessageId() {
    return messageId;
  }

  public void setMessageId(Long messageId) {
    this.messageId = messageId;
  }

  public ConsumerType getConsumerType() {
    return consumerType;
  }

  public void setConsumerType(ConsumerType consumerType) {
    this.consumerType = consumerType;
  }

  public Boolean getNeedClose() {
    return needClose;
  }

  public void setNeedClose(Boolean needClose) {
    this.needClose = needClose;
  }

  public Integer getThreadCount() {
    return threadCount;
  }

  public void setThreadCount(Integer threadCount) {
    this.threadCount = threadCount;
  }

  public MessageFilter getMessageFilter() {
    return messageFilter;
  }

  public void setMessageFilter(MessageFilter messageFilter) {
    this.messageFilter = messageFilter;
  }

  public String getIdcName() {
    return idcName;
  }

  public void setIdcName(String idcName) {
    this.idcName = idcName;
  }

  public String getZone() {
    return zone;
  }

  public void setZone(String zone) {
    this.zone = zone;
  }

  public String getPoolId() {
    return poolId;
  }

  public void setPoolId(String poolId) {
    this.poolId = poolId;
  }

  public MessageFilter getConsumeLocalZoneFilter() {
    return consumeLocalZoneFilter;
  }

  public void setConsumeLocalZoneFilter(MessageFilter consumeLocalZoneFilter) {
    this.consumeLocalZoneFilter = consumeLocalZoneFilter;
  }

  public ConsumerOffset getConsumerOffset() {
    return consumerOffset;
  }

  public void setConsumerOffset(ConsumerOffset consumerOffset) {
    this.consumerOffset = consumerOffset;
  }

  public Boolean getIsRestOffset() {
    return isRestOffset;
  }

  public void setIsRestOffset(Boolean isRestOffset) {
    this.isRestOffset = isRestOffset;
  }

}

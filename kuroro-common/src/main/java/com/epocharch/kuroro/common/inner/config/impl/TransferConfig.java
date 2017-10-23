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

import java.io.Serializable;
import java.util.Map;

/**
 * Transfer 全局参数配置
 *
 * @author pengrongxin
 */
public class TransferConfig implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = -2908627233908256313L;

  /**
   * Transfer为每个topic+consumerid启动的消费者个数
   */
  private int consumerNums;

  /**
   * 每个consumer的channel数
   */
  private int topicConsumerThreadNums;

  /**
   * transfer 生产线程数
   */
  private int transferWorkerThreadNums;

  /**
   * 每个线程对每个topicQueue上生产的消息的个数，值越大，停留在单个的topicQueue上的时间越长,
   * 意味着其他queue等待生产的时间也越长 越小，每个topicQueue都能得到及时处理，
   */
  private int transferWorkerProcessLevel;

  /**
   * 每个topic消息队列的上限，如果消息个数达到top，则阻塞消费
   */
  private int topicQueueTopLimit;

  /**
   * 每个topic消息队列的下限，如果消息个数达到bottom，则继续消费
   */
  private int topicQueueTopBottom;

  /**
   * transfer一次批量生产的消息的个数
   */
  private int transferBatchProductMsgNums;

  /**
   * transfer是否批量生产，默认是批量生产
   */
  private boolean transferBatchProducorOff;

  /**
   * transfer使用zone和zone之间的流量占比，最大是1，即100%
   */
  private Map<String, Float> zoneThrottlerMap;

  public Map<String, Float> getZoneThrottlerMap() {
    return zoneThrottlerMap;
  }

  public void setZoneThrottlerMap(Map<String, Float> zoneThrottlerMap) {
    this.zoneThrottlerMap = zoneThrottlerMap;
  }

  public int getTopicConsumerThreadNums() {
    return topicConsumerThreadNums;
  }

  public void setTopicConsumerThreadNums(int topicConsumerThreadNums) {
    this.topicConsumerThreadNums = topicConsumerThreadNums;
  }

  public int getTransferWorkerThreadNums() {
    return transferWorkerThreadNums;
  }

  public void setTransferWorkerThreadNums(int transferWorkerThreadNums) {
    this.transferWorkerThreadNums = transferWorkerThreadNums;
  }

  public int getTransferWorkerProcessLevel() {
    return transferWorkerProcessLevel;
  }

  public void setTransferWorkerProcessLevel(int transferWorkerProcessLevel) {
    this.transferWorkerProcessLevel = transferWorkerProcessLevel;
  }

  public int getTopicQueueTopLimit() {
    return topicQueueTopLimit;
  }

  public void setTopicQueueTopLimit(int topicQueueTopLimit) {
    this.topicQueueTopLimit = topicQueueTopLimit;
  }

  public int getTopicQueueTopBottom() {
    return topicQueueTopBottom;
  }

  public void setTopicQueueTopBottom(int topicQueueTopBottom) {
    this.topicQueueTopBottom = topicQueueTopBottom;
  }

  public int getTransferBatchProductMsgNums() {
    return transferBatchProductMsgNums;
  }

  public void setTransferBatchProductMsgNums(int transferBatchProductMsgNums) {
    this.transferBatchProductMsgNums = transferBatchProductMsgNums;
  }

  public boolean isTransferBatchProducorOff() {
    return transferBatchProducorOff;
  }

  public void setTransferBatchProducorOff(boolean transferBatchProducorOff) {
    this.transferBatchProducorOff = transferBatchProducorOff;
  }

  public void addZoneThrottle(String zone2zone, float size) {
    this.zoneThrottlerMap.put(zone2zone, size);
  }

  public int getConsumerNums() {
    return consumerNums;
  }

  public void setConsumerNums(int consumerNums) {
    this.consumerNums = consumerNums;
  }

}

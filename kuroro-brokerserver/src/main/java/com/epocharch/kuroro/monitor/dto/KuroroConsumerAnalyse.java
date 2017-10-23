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

import java.util.Date;

public class KuroroConsumerAnalyse {

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.Id
   */
  private Long id;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.TOPIC_NAME
   */
  private String topicName;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.BROKER
   */
  private String broker;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.OUT_COUNTS
   */
  private int outCounts;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.CONSUMER_NAME
   */
  private String consumerName;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.CONSUMER_IP
   */
  private String consumerIp;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.CONN_PORT
   */
  private Integer connPort;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.START_TIME
   */
  private Date startTime;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.END_TIME
   */
  private Date endTime;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_consumer_analyse.MEMO
   */
  private String memo;

  private String zone;

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.Id
   *
   * @return the value of monitor_kuroro_consumer_analyse.Id
   */
  public Long getId() {
    return id;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.Id
   *
   * @param id the value for monitor_kuroro_consumer_analyse.Id
   */
  public void setId(Long id) {
    this.id = id;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.TOPIC_NAME
   *
   * @return the value of monitor_kuroro_consumer_analyse.TOPIC_NAME
   */
  public String getTopicName() {
    return topicName;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.TOPIC_NAME
   *
   * @param topicName the value for monitor_kuroro_consumer_analyse.TOPIC_NAME
   */
  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.BROKER
   *
   * @return the value of monitor_kuroro_consumer_analyse.BROKER
   */
  public String getBroker() {
    return broker;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.BROKER
   *
   * @param broker the value for monitor_kuroro_consumer_analyse.BROKER
   */
  public void setBroker(String broker) {
    this.broker = broker;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.OUT_COUNTS
   *
   * @return the value of monitor_kuroro_consumer_analyse.OUT_COUNTS
   */
  public int getOutCounts() {
    return outCounts;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.OUT_COUNTS
   *
   * @param outCounts the value for monitor_kuroro_consumer_analyse.OUT_COUNTS
   */
  public void setOutCounts(Integer outCounts) {
    this.outCounts = outCounts;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.CONSUMER_NAME
   *
   * @return the value of monitor_kuroro_consumer_analyse.CONSUMER_NAME
   */
  public String getConsumerName() {
    return consumerName;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.CONSUMER_NAME
   *
   * @param consumerName the value for monitor_kuroro_consumer_analyse.CONSUMER_NAME
   */
  public void setConsumerName(String consumerName) {
    this.consumerName = consumerName;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.CONSUMER_IP
   *
   * @return the value of monitor_kuroro_consumer_analyse.CONSUMER_IP
   */
  public String getConsumerIp() {
    return consumerIp;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.CONSUMER_IP
   *
   * @param consumerIp the value for monitor_kuroro_consumer_analyse.CONSUMER_IP
   */
  public void setConsumerIp(String consumerIp) {
    this.consumerIp = consumerIp;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.CONN_PORT
   *
   * @return the value of monitor_kuroro_consumer_analyse.CONN_PORT
   */
  public Integer getConnPort() {
    return connPort;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.CONN_PORT
   *
   * @param connPort the value for monitor_kuroro_consumer_analyse.CONN_PORT
   */
  public void setConnPort(Integer connPort) {
    this.connPort = connPort;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.START_TIME
   *
   * @return the value of monitor_kuroro_consumer_analyse.START_TIME
   */
  public Date getStartTime() {
    return startTime;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.START_TIME
   *
   * @param startTime the value for monitor_kuroro_consumer_analyse.START_TIME
   */
  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.END_TIME
   *
   * @return the value of monitor_kuroro_consumer_analyse.END_TIME
   */
  public Date getEndTime() {
    return endTime;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.END_TIME
   *
   * @param endTime the value for monitor_kuroro_consumer_analyse.END_TIME
   */
  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_consumer_analyse.MEMO
   *
   * @return the value of monitor_kuroro_consumer_analyse.MEMO
   */
  public String getMemo() {
    return memo;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_consumer_analyse.MEMO
   *
   * @param memo the value for monitor_kuroro_consumer_analyse.MEMO
   */
  public void setMemo(String memo) {
    this.memo = memo;
  }

  public String getZone() {
    return zone;
  }

  public void setZone(String zone) {
    this.zone = zone;
  }

  public KuroroConsumerAnalyse merge(KuroroConsumerAnalyse consumerAnalyse) {
    if (consumerAnalyse != null) {
      this.outCounts = outCounts + consumerAnalyse.getOutCounts();
    }
    return this;
  }

  @Override
  public String toString() {
    return "KuroroConsumerAnalyse{" +
        "id=" + id +
        ", topicName='" + topicName + '\'' +
        ", broker='" + broker + '\'' +
        ", outCounts=" + outCounts +
        ", consumerName='" + consumerName + '\'' +
        ", consumerIp='" + consumerIp + '\'' +
        ", connPort=" + connPort +
        ", startTime=" + startTime +
        ", endTime=" + endTime +
        ", memo='" + memo + '\'' +
        ", zone='" + zone + '\'' +
        '}';
  }
}
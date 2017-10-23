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
import org.apache.commons.lang.StringUtils;

public class KuroroProducerAnalyse {

  /**
   * This field corresponds to the database column
   * monitor_kuroro_producer_analyse_hour.Id
   */
  private Long id;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_producer_analyse_hour.TOPIC_NAME
   */
  private String topicName;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_producer_analyse_hour.BROKER
   */
  private String broker;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_producer_analyse_hour.IN_COUNTS
   */
  private int inCounts;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_producer_analyse_hour.START_TIME
   */
  private Date startTime;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_producer_analyse_hour.END_TIME
   */
  private Date endTime;

  /**
   * This field corresponds to the database column
   * monitor_kuroro_producer_analyse_hour.MEMO
   */
  private String memo;

  /*
  * this field corresponds to the database cloumn
  * monitor_kuroro_producer_analyse_hour.ZONE
  *
  * */
  private String zone;

  /**
   * This method returns the value of the database column
   * monitor_kuroro_producer_analyse_hour.Id
   *
   * @return the value of monitor_kuroro_producer_analyse_hour.Id
   */
  public Long getId() {
    return id;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_producer_analyse_hour.Id
   *
   * @param id the value for monitor_kuroro_producer_analyse_hour.Id
   */
  public void setId(Long id) {
    this.id = id;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_producer_analyse_hour.TOPIC_NAME
   *
   * @return the value of monitor_kuroro_producer_analyse_hour.TOPIC_NAME
   */
  public String getTopicName() {
    return topicName;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_producer_analyse_hour.TOPIC_NAME
   *
   * @param topicName the value for monitor_kuroro_producer_analyse_hour.TOPIC_NAME
   */
  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_producer_analyse_hour.BROKER
   *
   * @return the value of monitor_kuroro_producer_analyse_hour.BROKER
   */
  public String getBroker() {
    return broker;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_producer_analyse_hour.BROKER
   *
   * @param broker the value for monitor_kuroro_producer_analyse_hour.BROKER
   */
  public void setBroker(String broker) {
    this.broker = broker;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_producer_analyse_hour.IN_COUNTS
   *
   * @return the value of monitor_kuroro_producer_analyse_hour.IN_COUNTS
   */
  public Integer getInCounts() {
    return inCounts;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_producer_analyse_hour.IN_COUNTS
   *
   * @param inCounts the value for monitor_kuroro_producer_analyse_hour.IN_COUNTS
   */
  public void setInCounts(Integer inCounts) {
    this.inCounts = inCounts;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_producer_analyse_hour.START_TIME
   *
   * @return the value of monitor_kuroro_producer_analyse_hour.START_TIME
   */
  public Date getStartTime() {
    return startTime;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_producer_analyse_hour.START_TIME
   *
   * @param startTime the value for monitor_kuroro_producer_analyse_hour.START_TIME
   */
  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_producer_analyse_hour.END_TIME
   *
   * @return the value of monitor_kuroro_producer_analyse_hour.END_TIME
   */
  public Date getEndTime() {
    return endTime;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_producer_analyse_hour.END_TIME
   *
   * @param endTime the value for monitor_kuroro_producer_analyse_hour.END_TIME
   */
  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  /**
   * This method returns the value of the database column
   * monitor_kuroro_producer_analyse_hour.MEMO
   *
   * @return the value of monitor_kuroro_producer_analyse_hour.MEMO
   */
  public String getMemo() {
    return memo;
  }

  /**
   * This method sets the value of the database column
   * monitor_kuroro_producer_analyse_hour.MEMO
   *
   * @param memo the value for monitor_kuroro_producer_analyse_hour.MEMO
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

  public KuroroProducerAnalyse merge(KuroroProducerAnalyse kuroroProducerAnalyse) {
    if (kuroroProducerAnalyse != null && StringUtils
        .equals(broker, kuroroProducerAnalyse.getBroker())) {
      this.inCounts = inCounts + kuroroProducerAnalyse.getInCounts();
    }
    return this;
  }
}
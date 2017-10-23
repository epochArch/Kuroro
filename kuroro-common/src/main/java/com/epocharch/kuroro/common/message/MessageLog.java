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

package com.epocharch.kuroro.common.message;

import java.util.Date;
import java.util.List;

public class MessageLog {

  public static final String TOPIC_ANALYSE = "kuroroAnalyse";

  private String topicName;

  private String broker;

  private String consumerName;

  private Long time;

  private Long in;

  private Long inSize;

  private List<OutMessagePair> out;

  private String zoneName;

  private String idcName;

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

  public String getConsumerName() {
    return consumerName;
  }

  public void setConsumerName(String consumerName) {
    this.consumerName = consumerName;
  }

  public Long getTime() {
    return time;
  }

  public void setTime(Long time) {
    this.time = time;
  }

  public Long getIn() {
    return in;
  }

  public void setIn(Long in) {
    this.in = in;
  }

  public List<OutMessagePair> getOut() {
    return out;
  }

  public void setOut(List<OutMessagePair> out) {
    this.out = out;
  }

  public Date fetchAnalystTime() {
    if (time != 0L) {
      return new Date(time);
    }

    return new Date();
  }

  public Long getInSize() {
    return inSize;
  }

  public void setInSize(Long inSize) {
    this.inSize = inSize;
  }

  public String getZoneName() {
    return zoneName;
  }

  public void setZoneName(String zoneName) {
    this.zoneName = zoneName;
  }

  public String getIdcName() {
    return idcName;
  }

  public void setIdcName(String idcName) {
    this.idcName = idcName;
  }

}

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
import java.util.concurrent.atomic.AtomicInteger;

public class KuroroSumInfo implements Serializable  {


  private static final long serialVersionUID = 4393671700265164898L;
  private String topicName;
  private String consumerName;
  private AtomicInteger inCounts = new AtomicInteger(0);
  private AtomicInteger outCounts = new AtomicInteger(0);

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public AtomicInteger getInCounts() {
    return inCounts;
  }

  public void setInCounts(AtomicInteger inCounts) {
    this.inCounts = inCounts;
  }

  public void addInCounts(int in) {
    inCounts.addAndGet(in);
  }

  public void addOutCounts(int out) {
    outCounts.addAndGet(out);
  }

  public AtomicInteger getOutCounts() {
    return outCounts;
  }

  public void setOutCounts(AtomicInteger outCounts) {
    this.outCounts = outCounts;
  }

  public String getConsumerName() {
    return consumerName;
  }

  public void setConsumerName(String consumerName) {
    this.consumerName = consumerName;
  }
}

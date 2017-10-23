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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CoumserOutInfo {

  private String consumerName;
  private Map<String, AtomicLong> consumerHosts = new HashMap<String, AtomicLong>();

  public String getConsumerName() {
    return consumerName;
  }

  public void setConsumerName(String consumerName) {
    this.consumerName = consumerName;
  }

  public Map<String, AtomicLong> getConsumerHosts() {
    return consumerHosts;
  }

  public void setConsumerHosts(Map<String, AtomicLong> consumerHosts) {
    this.consumerHosts = consumerHosts;
  }

  public void addReceiveCounts(String host, long counts) {
    AtomicLong atomicLong = consumerHosts.get(host);
    if (atomicLong == null) {
      atomicLong = new AtomicLong(0);
      consumerHosts.put(host, atomicLong);
    }
    atomicLong.addAndGet(counts);
  }
}

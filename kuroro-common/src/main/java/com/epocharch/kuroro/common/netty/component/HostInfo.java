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

package com.epocharch.kuroro.common.netty.component;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class HostInfo implements Serializable {

  private static final long serialVersionUID = 5698960019171571461L;
  private String connect;
  private String host;
  private int port;
  private int weight = 1;

  private AtomicInteger curWeight = new AtomicInteger(weight);

  public HostInfo(String host, int port, int weight) {
    this.host = host;
    this.port = port;
    this.connect = host + ":" + port;
    this.weight = weight;
  }

  public HostInfo(String connect, int weight) {
    int colonIdx = connect.indexOf(":");
    this.connect = connect;
    this.host = connect.substring(0, colonIdx);
    this.port = Integer.parseInt(connect.substring(colonIdx + 1));
    this.weight = weight;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HostInfo) {
      HostInfo hp = (HostInfo) obj;
      return this.host.equals(hp.host) && this.port == hp.port;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return host.hashCode() + port;
  }

  @Override
  public String toString() {
    return "HostInfo [host=" + host + ", port=" + port + ", weight=" + weight + "]";
  }

  public String getConnect() {
    return connect;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
    this.curWeight = new AtomicInteger(weight);
  }

  public void setCurWeight(AtomicInteger curWeight) {
    this.curWeight = curWeight;
  }

  public void decreaseCurWeight() {
    curWeight.decrementAndGet();
  }

  public void resetCurWeight() {
    curWeight = new AtomicInteger(weight);
  }

  public int getCurWeighted() {
    return curWeight.getAndDecrement();
  }

}

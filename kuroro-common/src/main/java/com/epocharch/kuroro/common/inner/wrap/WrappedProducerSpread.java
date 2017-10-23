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

public final class WrappedProducerSpread extends Wrap {

  private static final long serialVersionUID = -7036273844451797425L;
  private String producerIP;
  private String producerVersion;

  public WrappedProducerSpread() {

  }

  public WrappedProducerSpread(String producerVersion, String productIP) {
    setWrappedType(WrappedType.CONSUMER_SPREAD);
    setProducerVersion(producerVersion);
    setProducerIP(producerIP);
  }

  public String getProducerIP() {
    return producerIP;
  }

  public void setProducerIP(String producerIP) {
    this.producerIP = producerIP;
  }

  public String getProducerVersion() {
    return producerVersion;
  }

  public void setProducerVersion(String producerVersion) {
    this.producerVersion = producerVersion;
  }

}

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

public final class WrappedProducerAck extends Wrap {

  private static final long serialVersionUID = -8165453930650914399L;
  private String shaInfo;
  private int status;

  public WrappedProducerAck(String shaInfo) {
    setWrappedType(WrappedType.PRODUCER_ACK);
    setShaInfo(shaInfo);
  }

  public WrappedProducerAck() {
    super();
  }

  public WrappedProducerAck(String shaInfo, int status) {
    super();
    setWrappedType(WrappedType.PRODUCER_ACK);
    setShaInfo(shaInfo);
    setStatus(status);
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getShaInfo() {
    return shaInfo;
  }

  public void setShaInfo(String shaInfo) {
    this.shaInfo = shaInfo;
  }

}

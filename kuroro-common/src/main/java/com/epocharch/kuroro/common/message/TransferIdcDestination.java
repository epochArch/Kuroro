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

import com.epocharch.kuroro.common.inner.util.NameCheckUtil;

/**
 * Created by zhoufeiqiang on 09/03/2017.
 */
public class TransferIdcDestination extends Destination {

  public static final String SEPARATOR = "@";
  private static final long serialVersionUID = 3833312742447701703L;
  private String idc;
  private String topic;
  private String consumerId;

  TransferIdcDestination() {
  }

  private TransferIdcDestination(String topic, String idc, String consumerId) {

    this.topic = topic.trim();
    this.idc = idc;
    this.consumerId = consumerId;
  }

  public static TransferIdcDestination topic(String topic, String idc, String consumerId) {
    if (!NameCheckUtil.isTopicNameValid(topic)) {
      throw new IllegalArgumentException((new StringBuilder())
          .append(
              "Topic name is illegal, should be [0-9,a-z,A-Z,'_','-'], begin with a letter, and length is 2-30 long\uFF1A")
          .append(topic).toString());
    }

    if (!NameCheckUtil.isConsumerIdValid(consumerId)) {
      throw new IllegalArgumentException((new StringBuilder())
          .append(
              "Consumer id is illegal, should be [0-9,a-z,A-Z,'_','-'], begin with a letter, and length is 2-30 long\uFF1A")
          .append(topic).toString());
    }

    return new TransferIdcDestination(topic, idc, consumerId);

  }

  /**
   * 获取目标zone的topic name
   */
  public String getName() {
    if (idc != null && !idc.isEmpty()) {
      return topic + SEPARATOR + idc + SEPARATOR + consumerId;
    }
    return null;
  }

  /**
   * 获取归属topic
   */
  public String getTopic() {
    return topic;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public String toString() {
    return String
        .format("Destination [name=%s,idc=%s,consumerId=%s]", new Object[]{topic, idc, consumerId});
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TransferIdcDestination that = (TransferIdcDestination) o;

    if (consumerId != null ? !consumerId.equals(that.consumerId) : that.consumerId != null) {
      return false;
    }
    if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
      return false;
    }
    if (idc != null ? !idc.equals(that.idc) : that.idc != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (idc != null ? idc.hashCode() : 0);
    result = 31 * result + (topic != null ? topic.hashCode() : 0);
    result = 31 * result + (consumerId != null ? consumerId.hashCode() : 0);
    return result;
  }
}

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

import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.NameCheckUtil;

/**
 * Transfer专用Destination
 */
public class TransferDestination extends Destination {

  public static final String SEPARATOR = "@";
  private static final long serialVersionUID = 3833312742447701703L;
  //zone design
  private String zoneOrIdc;
  private String topic;
  private String consumerId;

  TransferDestination() {
  }

  private TransferDestination(String topic, String zoneOrIdc, String consumerId) {

    this.topic = topic.trim();
    this.zoneOrIdc = zoneOrIdc;
    this.consumerId = consumerId;
  }

  public static TransferDestination topic(String topic, String zoneOrIdc, String consumerId) {
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

    if (KuroroUtil.isBlankString(zoneOrIdc)) {
      /*String localIdcName = KuroroZkUtil.localIDCName();
            if (localIdcName.contains(Constants.IDC_NH)) {
                zoneOrIdc = Constants.ZONE_HEAD + Constants.IDC_NH;
            }else if (localIdcName.contains(Constants.IDC_BJ)) {
                zoneOrIdc = Constants.ZONE_HEAD + Constants.IDC_BJ;
            }else if (localIdcName.contains(Constants.IDC_JQ)) {
                zoneOrIdc = Constants.ZONE_HEAD + Constants.IDC_JQ;
            }*/
    }
    if (!(KuroroZkUtil.allIDCName().contains(zoneOrIdc) || KuroroZkUtil.allIdcZoneName()
        .contains(zoneOrIdc))) {
      throw new IllegalArgumentException("Invalid idc or zone name : " + zoneOrIdc);
    }

    return new TransferDestination(topic, zoneOrIdc, consumerId);

  }

  /**
   * 获取目标zone的topic name
   */
  public String getName() {
    if (zoneOrIdc != null && !zoneOrIdc.isEmpty()) {
      return topic + SEPARATOR + zoneOrIdc + SEPARATOR + consumerId;
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
    return String.format("Destination [name=%s,zoneOrIdc=%s,consumerId=%s]",
        new Object[]{topic, zoneOrIdc, consumerId});
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

    TransferDestination that = (TransferDestination) o;

    if (consumerId != null ? !consumerId.equals(that.consumerId) : that.consumerId != null) {
      return false;
    }
    if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
      return false;
    }
    if (zoneOrIdc != null ? !zoneOrIdc.equals(that.zoneOrIdc) : that.zoneOrIdc != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (zoneOrIdc != null ? zoneOrIdc.hashCode() : 0);
    result = 31 * result + (topic != null ? topic.hashCode() : 0);
    result = 31 * result + (consumerId != null ? consumerId.hashCode() : 0);
    return result;
  }
}

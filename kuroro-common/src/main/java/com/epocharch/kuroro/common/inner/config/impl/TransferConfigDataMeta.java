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

package com.epocharch.kuroro.common.inner.config.impl;

import java.io.Serializable;

public class TransferConfigDataMeta implements Serializable {

  private static final long serialVersionUID = 6083301715588768469L;
  private String topicName;
  private String regionName;
  private String ipList;
  private String messageFilter;
  private Boolean isAsync;

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public String getIpList() {
    return ipList;
  }

  public void setIpList(String ipList) {
    this.ipList = ipList;
  }

  public String getMessageFilter() {
    return messageFilter;
  }

  public void setMessageFilter(String messageFilter) {
    this.messageFilter = messageFilter;
  }

  public Boolean getIsAsync() {
    return isAsync;
  }

  public void setIsAsync(Boolean isAsync) {
    this.isAsync = isAsync;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((regionName == null) ? 0 : regionName.hashCode());
    result = prime * result + ((topicName == null) ? 0 : topicName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TransferConfigDataMeta other = (TransferConfigDataMeta) obj;
    if (regionName == null) {
      if (other.regionName != null) {
        return false;
      }
    } else if (!regionName.equals(other.regionName)) {
      return false;
    }
    if (topicName == null) {
      if (other.topicName != null) {
        return false;
      }
    } else if (!topicName.equals(other.topicName)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "TransferConfigDataMeta [topicName=" + topicName + ", regionName=" + regionName
        + ", ipList=" + ipList + ", messageFilter="
        + messageFilter + ", isAsync=" + isAsync + "]";
  }
}

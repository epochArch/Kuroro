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

package com.epocharch.kuroro.broker.consumerserver.impl;

import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.ConsumerType;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.message.Destination;

public class Consumer {

  private String consumerId;
  private Destination destination;
  private ConsumerType consumerType;
  private String idc;
  private String zone;
  private String poolId;
  private MessageFilter consumerLocalZoneFilter;
  private ConsumerOffset consumerOffset;

  public Consumer(String consumerId, Destination destination, ConsumerType consumerType) {
    this.consumerId = consumerId;
    this.destination = destination;
    this.consumerType = consumerType;

  }

  public Consumer(String consumerId, Destination destination, ConsumerType consumerType, String idc,
      String zone, String poolId) {
    this.consumerId = consumerId;
    this.destination = destination;
    this.consumerType = consumerType;
    this.idc = idc;
    this.zone = zone;
    this.poolId = poolId;

  }

  public Consumer(String consumerId, Destination destination, ConsumerType consumerType, String idc,
      String zone, String poolId,
      MessageFilter consumerLocalZoneFilter, ConsumerOffset offset) {
    this.consumerId = consumerId;
    this.destination = destination;
    this.consumerType = consumerType;
    this.idc = idc;
    this.zone = zone;
    this.poolId = poolId;
    this.consumerLocalZoneFilter = consumerLocalZoneFilter;
    this.consumerOffset = offset;

  }

  public ConsumerType getConsumerType() {
    return consumerType;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public Destination getDestination() {
    return destination;
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((consumerId == null) ? 0 : consumerId.hashCode());
    result = prime * result + ((destination == null) ? 0 : destination.hashCode());
    result = prime * result + ((consumerLocalZoneFilter == null) ? 0
        : consumerLocalZoneFilter.getParam().hashCode());
    result = prime * result + ((consumerType == null) ? 0 : consumerType.hashCode());
    result = prime * result + ((consumerOffset == null) ? 0 : consumerOffset.getType().hashCode());
    result =
        prime * result + (((consumerOffset != null) && (consumerOffset.getOffsetValue() != null)) ?
            consumerOffset.getOffsetValue().hashCode() :
            0);
    return result;
  }

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
    Consumer other = (Consumer) obj;
    if (consumerId == null) {
      if (other.consumerId != null) {
        return false;
      }
    } else if (!consumerId.equals(other.consumerId)) {
      return false;
    }
    if (destination == null) {
      if (other.destination != null) {
        return false;
      }
    } else if (!destination.equals(other.destination)) {
      return false;
    }

    if (consumerLocalZoneFilter == null) {
      if (other.consumerLocalZoneFilter != null) {
        return false;
      }
    } else if (!consumerLocalZoneFilter.getParam()
        .equals(other.consumerLocalZoneFilter.getParam())) {
      return false;
    }

    if (consumerType == null) {
      if (other.consumerType != null) {
        return false;
      } else if (!consumerType.equals(other.consumerType)) {
        return false;
      }
    }

    if (consumerOffset == null) {
      if (other.consumerOffset != null) {
        return false;
      }
    } else if (!consumerOffset.getType().equals(other.consumerOffset.getType())) {
      return false;
    }

    if (consumerOffset != null && other.consumerOffset != null) {
      if (consumerOffset.getOffsetValue() == null) {
        if (other.consumerOffset.getOffsetValue() != null) {
          return false;
        }
      } else if (!consumerOffset.getOffsetValue().equals(other.consumerOffset.getOffsetValue())) {
        return false;
      }
    }

    return true;
  }

  public String toString() {
    return String
        .format(
            "Consumer [consumerId=%s, dest=%s, idc=%s, zone=%s,poolId=%s,consumeLocalZoneFilter=%s,consumerType=%s, consumerOffset=%s]",
            consumerId, destination, idc, zone, poolId, consumerLocalZoneFilter, consumerType,
            consumerOffset);
  }

  public String getIdc() {
    return idc;
  }

  public String getZone() {
    return zone;
  }

  public String getPoolId() {
    return poolId;
  }

  public ConsumerOffset getConsumerOffset() {
    return consumerOffset;
  }
}

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

package com.epocharch.kuroro.common.consumer;

import java.io.Serializable;

/**
 * Created by zhoufeiqiang on 23/06/2017.
 */
public class ConsumerOffset implements Serializable {

  private static final long serialVersionUID = 3808835228367292265L;
  private Long offsetValue = null;
  private OffsetType type;
  private Boolean isRestOffset = false;
  private String dateTime;

  public ConsumerOffset() {

  }

  public ConsumerOffset(String dateTime) {
    this.dateTime = dateTime;
  }

  public ConsumerOffset(OffsetType type) {
    this.type = type;
  }

  public Long getOffsetValue() {
    return offsetValue;
  }

  public void setOffsetValue(Long offsetValue) {
    this.offsetValue = offsetValue;
  }

  public OffsetType getType() {
    return type;
  }

  public void setType(OffsetType type) {
    this.type = type;
  }

  public Boolean getRestOffset() {
    return isRestOffset;
  }

  public void setRestOffset(Boolean restOffset) {
    isRestOffset = restOffset;
  }

  public String getDateTime() {
    return dateTime;
  }

  public void setDateTime(String dateTime) {
    this.dateTime = dateTime;
  }

  @Override
  public String toString() {
    return "ConsumerOffset{" + "offsetValue=" + offsetValue + ", type=" + type + ", isRestOffset="
        + isRestOffset + ", dateTime='"
        + dateTime + '\'' + '}';
  }

  public enum OffsetType {
    /**
     * consumerclient consume message from current message on mongodb
     */
    LARGEST_OFFSET,

    /**
     * consumerclient consume message from smallest message on mongodb
     */
    SMALLEST_OFFSET,

    /**
     * consumer pool costomized offset value
     */
    CUSTOMIZE_OFFSET
  }

}

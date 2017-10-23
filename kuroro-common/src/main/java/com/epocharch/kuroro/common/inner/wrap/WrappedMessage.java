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

import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.message.Destination;
import java.util.List;

public class WrappedMessage extends Wrap implements Message {

  public static final WrappedMessage HEARTBEAT = new WrappedMessage(WrappedType.HEARTBEAT);
  /**
   *
   */
  private static final long serialVersionUID = -4461557591355500006L;
  private KuroroMessage content;
  //保证协议兼容
  private List<KuroroMessage> batchContent;
  private Destination destination;
  private String monitorEventID;
  private Boolean isRestOffset;
  private WrappedType type;

  public WrappedMessage() {
    super.setWrappedType(WrappedType.OBJECT_MSG);
  }

  public WrappedMessage(WrappedType type) {
    super.setWrappedType(type);
  }

  public WrappedMessage(WrappedType type, Boolean isRestOffset) {
    this.type = type;
    this.isRestOffset = isRestOffset;
  }

  public WrappedMessage(Destination dest, KuroroMessage content) {
    super.setWrappedType(WrappedType.OBJECT_MSG);
    this.setDestination(dest);
    this.setContent(content);
  }

  public WrappedMessage(Destination dest, KuroroMessage content, WrappedType type) {
    super.setWrappedType(type);
    this.setDestination(dest);
    this.setContent(content);
  }

  @Override
  public KuroroMessage getContent() {
    return content;
  }

  public void setContent(KuroroMessage content) {
    this.content = content;
  }

  @Override
  public Destination getDestination() {
    return destination;
  }

  public void setDestination(Destination destination) {
    this.destination = destination;
  }

  @Override
  public String toString() {
    return String.format("WrappedMessage [content=%s, dest=%s]", getContent(), getDestination());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getContent() == null) ? 0 : getContent().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WrappedMessage that = (WrappedMessage) o;

    if (batchContent != null ? !batchContent.equals(that.batchContent)
        : that.batchContent != null) {
      return false;
    }
    if (content != null ? !content.equals(that.content) : that.content != null) {
      return false;
    }
    if (!destination.equals(that.destination)) {
      return false;
    }
    if (monitorEventID != null ? !monitorEventID.equals(that.monitorEventID)
        : that.monitorEventID != null) {
      return false;
    }

    return true;
  }

  public String getMonitorEventID() {
    return monitorEventID;
  }

  public void setMonitorEventID(String monitorEventID) {
    this.monitorEventID = monitorEventID;
  }

  public List<KuroroMessage> getBatchContent() {
    return batchContent;
  }

  public void setBatchContent(List<KuroroMessage> batchContent) {
    this.batchContent = batchContent;
  }

  public Boolean getRestOffset() {
    return isRestOffset;
  }

  public void setRestOffset(Boolean restOffset) {
    isRestOffset = restOffset;
  }

}

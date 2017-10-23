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

package com.epocharch.kuroro.common.inner.message;

import com.epocharch.kuroro.common.ProtocolType;
import com.epocharch.kuroro.common.message.Message;
import com.epocharch.kuroro.common.protocol.CachedData;
import com.epocharch.kuroro.common.protocol.Transcoder;
import com.epocharch.kuroro.common.protocol.hessian.HessianTranscoder;
import com.epocharch.kuroro.common.protocol.json.JsonBinder;
import com.epocharch.kuroro.common.protocol.json.TypedJsonBinder;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhoufeiqiang on 10/10/2017.
 */
public class KuroroMessage implements Serializable, Message {

  private static final long serialVersionUID = 434748017083198438L;
  private Date generatedTime;
  private Long messageId;
  private Map<String, String> properties;
  private Map<String, String> internalProperties;
  private String version;
  private String content;
  private String sha1;
  private String type;
  private String sourceIp;
  private String protocolType;
  private String createdTime;
  private String poolId;
  private String zone;
  private String idcName;
  private Object messageContent;

  public KuroroMessage() {
  }

  @Override
  public String getProtocolType() {
    return protocolType;
  }

  public void setProtocolType(String protocolType) {
    this.protocolType = protocolType;
  }

  @Override
  public String getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(String createdTime) {
    this.createdTime = createdTime;
  }

  @Override
  public Date getGeneratedTime() {
    return generatedTime;
  }

  public void setGeneratedTime(Date generatedTime) {
    this.generatedTime = generatedTime;
  }

  @Override
  public Long getMessageId() {
    return messageId;
  }

  public void setMessageId(Long messageId) {
    this.messageId = messageId;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public Map<String, String> getInternalProperties() {
    return internalProperties;
  }

  public void setInternalProperties(Map<String, String> internalProperties) {
    this.internalProperties = internalProperties;
  }

  @Override
  public String getSha1() {
    return sha1;
  }

  public void setSha1(String sha1) {
    this.sha1 = sha1;
  }

  public Object getMessageContent() {
    return messageContent;
  }

  public void setMessageContent(Object messageContent) {
    this.messageContent = messageContent;
  }

  @SuppressWarnings("unchecked")
  @Override
  // from store
  public <T> T transferContentToBean(Class<T> clazz) {
    JsonBinder jsonBinder = JsonBinder.getNonEmptyBinder();
    if (protocolType != null) {
      if (protocolType.equals(ProtocolType.JSON.toString())) {
        return jsonBinder.fromJson(this.content, clazz);
      } else if (protocolType.equals(ProtocolType.TYPED_JSON.toString())) {
        TypedJsonBinder typedJsonBinder = TypedJsonBinder.getNonEmptyBinder();
        return typedJsonBinder.fromJson(this.content, clazz);
      }
    }
    //default hessian
    Transcoder transcoder = HessianTranscoder.getTranscoder();
    Map<String, Object> c = jsonBinder.fromJson(this.content, HashMap.class);
    byte[] data = jsonBinder.fromJson(c.get("data").toString(), byte[].class);
    CachedData cachedData = new CachedData(data, (Integer) c.get("flag"));
    return (T) transcoder.decode(cachedData);
  }

  @Override
  public String getContent() {
    return content;
  }

  // from store
  public void setContent(Object content) {
    if (content instanceof String) {
      this.content = (String) content;
    }
  }

  // from send
  public void setContentFromSend(Object content, int hessianCompressionThreshold) {
    JsonBinder jsonBinder = JsonBinder.getNonEmptyBinder();

    if (content instanceof String) {
      this.content = jsonBinder.toJson(content);
      setProtocolType(ProtocolType.JSON.toString());
    } else {
      if (protocolType != null) {
        if (protocolType.equals(ProtocolType.JSON.toString())) {
          this.content = jsonBinder.toJson(content);
          return;
        } else if (protocolType.equals(ProtocolType.TYPED_JSON.toString())) {
          TypedJsonBinder typedJsonBinder = TypedJsonBinder.getNonEmptyBinder();
          this.content = typedJsonBinder.toJson(content);
          return;
        }
      }
      //default hessian
      setProtocolType(ProtocolType.HESSIAN.toString());
      Transcoder transcoder = HessianTranscoder.getTranscoder();
      transcoder.setCompressionThreshold(hessianCompressionThreshold);
      this.content = convertHessian(jsonBinder, transcoder.encode(content));
    }
  }

  private String convertHessian(JsonBinder jb, CachedData data) {
    String content = jb.toJson(data.getData());
    Map<String, Object> x = new HashMap<String, Object>();
    x.put("data", content);
    x.put("flag", data.getFlag());
    return jb.toJson(x);
  }

  @Override
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String getSourceIp() {
    return sourceIp;
  }

  public void setSourceIp(String sourceIp) {
    this.sourceIp = sourceIp;
  }

  @Override
  public String getPoolId() {
    return poolId;
  }

  public void setPoolId(String poolId) {
    this.poolId = poolId;
  }

  @Override
  public String getZone() {
    return zone;
  }

  public void setZone(String zone) {
    this.zone = zone;
  }

  public String getIdcName() {
    return idcName;
  }

  public void setIdcName(String idcName) {
    this.idcName = idcName;
  }

  @Override
  public String toString() {
    return "KuroroMessage [generatedTime=" + generatedTime + ", messageId=" + messageId
        + ", properties=" + properties
        + ", internalProperties=" + internalProperties + ", version=" + version + ", sha1=" + sha1
        + ", type=" + type
        + ", sourceIp=" + sourceIp + ", createdTime=" + createdTime + " , poolId=" + poolId
        + ", zone=" + zone + " , idcName= "
        + idcName + " , messageContent=" + messageContent + "]";
  }

  public String toKeyValuePairs() {
    return toSuccessKeyValuePairs() + "&content=" + content;
  }

  public String toSuccessKeyValuePairs() {
    return "generatedTime=" + generatedTime + "&messageId=" + messageId + "&properties="
        + properties + "&internalProperties="
        + internalProperties + "&version=" + version + "&sha1=" + sha1 + "&type=" + type
        + "&sourceIp=" + sourceIp + "&createdTime="
        + createdTime + "&poolId=" + poolId + "&zone=" + zone + " &idcName=" + idcName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
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
    KuroroMessage other = (KuroroMessage) obj;
    if (messageId == null) {
      if (other.messageId != null) {
        return false;
      }
    } else if (!messageId.equals(other.messageId)) {
      return false;
    }
    return true;
  }

  /**
   * 在不比较MessageId的情况下，判断消息是否相等。
   */
  public boolean equalsWithoutMessageId(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KuroroMessage)) {
      return false;
    }
    KuroroMessage other = (KuroroMessage) obj;
    if (content == null) {
      if (other.content != null) {
        return false;
      }
    } else if (!content.equals(other.content)) {
      return false;
    }
    if (generatedTime == null) {
      if (other.generatedTime != null) {
        return false;
      }
    } else if (!generatedTime.equals(other.generatedTime)) {
      return false;
    }
    if (properties == null) {
      if (other.properties != null) {
        return false;
      }
    } else if (!properties.equals(other.properties)) {
      return false;
    }
    if (internalProperties == null) {
      if (other.internalProperties != null) {
        return false;
      }
    } else if (!internalProperties.equals(other.internalProperties)) {
      return false;
    }
    if (sha1 == null) {
      if (other.sha1 != null) {
        return false;
      }
    } else if (!sha1.equals(other.sha1)) {
      return false;
    }
    if (sourceIp == null) {
      if (other.sourceIp != null) {
        return false;
      }
    } else if (!sourceIp.equals(other.sourceIp)) {
      return false;
    }
    if (type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!type.equals(other.type)) {
      return false;
    }
    if (version == null) {
      if (other.version != null) {
        return false;
      }
    } else if (!version.equals(other.version)) {
      return false;
    }
    if (createdTime == null) {
      if (other.createdTime != null) {
        return false;
      }
    } else if (!createdTime.equals(other.createdTime)) {
      return false;
    }
    return true;
  }

}

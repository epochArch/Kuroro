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

package com.epocharch.kuroro.common.protocol.json;

import com.epocharch.kuroro.common.message.JsonDeserializedException;
import com.epocharch.kuroro.common.message.JsonSerializedException;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;

/**
 * 简单封装Jackson，实现JSON String<->Java Object的Mapper.
 */
public final class JsonBinder {

  private ObjectMapper mapper;

  private JsonBinder(Include include) {
    mapper = new ObjectMapper();
    // 设置输出时包含属性的风格
    mapper.setSerializationInclusion(include);
    // 序列化时，忽略空的bean(即沒有任何Field)
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    // 序列化时，忽略在JSON字符串中存在但Java对象实际没有的属性
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    // make all member fields serializable without further annotations,
    // instead of just public fields (default setting).
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
  }

  /**
   * 获取只输出非Null且非Empty(如List.isEmpty)的属性到Json字符串的Mapper,建议在外部接口中使用.
   */
  public static JsonBinder getNonEmptyBinder() {
    return NonEmptySingletonHolder.nonEmptyBinder;
  }

  /**
   * Object可以是POJO，也可以是Collection或数组。 如果对象为Null, 返回"null". 如果集合为空集合, 返回"[]".
   */
  public String toJson(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (IOException e) {
      throw new JsonSerializedException("Serialized Object to json string error : " + object, e);
    }
  }

  /**
   * 反序列化POJO或简单Collection如List<String>. 如果JSON字符串为Null或"null"字符串, 返回Null.
   * 如果JSON字符串为"[]", 返回空集合. 如需反序列化复杂Collection如List<MyBean>,
   * 请使用fromJson(String,JavaType)
   *
   * @see #fromJson(String, JavaType)
   */
  public <T> T fromJson(String jsonString, Class<T> clazz) {
    if (jsonString == null || "".equals(jsonString.trim())) {
      return null;
    }
    try {
      return mapper.readValue(jsonString, clazz);
    } catch (IOException e) {
      throw new JsonDeserializedException("Deserialized json string error : " + jsonString, e);
    }
  }

  private static class NonEmptySingletonHolder {

    public static final JsonBinder nonEmptyBinder = new JsonBinder(Include.NON_EMPTY);
  }
}

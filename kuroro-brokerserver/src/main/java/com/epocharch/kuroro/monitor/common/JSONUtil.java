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

package com.epocharch.kuroro.monitor.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("unchecked")
public class JSONUtil {

  /**
   * @see java.lang.Boolean#TYPE
   * @see java.lang.Character#TYPE
   * @see java.lang.Byte#TYPE
   * @see java.lang.Short#TYPE
   * @see java.lang.Integer#TYPE
   * @see java.lang.Long#TYPE
   * @see java.lang.Float#TYPE
   * @see java.lang.Double#TYPE
   * @see java.lang.Void#TYPE
   */
  @SuppressWarnings("rawtypes")
  static final Set<Class> simpleTypeSet = new HashSet<Class>() {
    private static final long serialVersionUID = 1L;

    {
      add(Boolean.class);
      add(Character.class);
      add(Byte.class);
      add(Short.class);
      add(Integer.class);
      add(Long.class);
      add(Float.class);
      add(java.util.Date.class);
      add(java.sql.Date.class);
      add(String.class);
      add(StringBuffer.class);
      add(StringBuilder.class);
      add(BigDecimal.class);
      add(BigInteger.class);
      add(Class.class);

      add(AtomicBoolean.class);
      add(AtomicInteger.class);
      add(AtomicLong.class);

      add(Integer.TYPE);
      add(Boolean.TYPE);
      add(Character.TYPE);
      add(Byte.TYPE);
      add(Short.TYPE);
      add(Long.TYPE);
      add(Float.TYPE);

      add(Class.class);

    }
  };

  /**
   * toEntity:将对象转换为指定类型的实体
   *
   * @return T
   */
  @SuppressWarnings("rawtypes")
  public static <T> T toEntity(Object source, Class<T> targetClass) {
    if (source == null || "null".equals(source.toString())) {
      return null;
    }
    Class clazz = source.getClass();
    if (targetClass == clazz) {
      return (T) source;
    }
    if (clazz == String.class) {
      if (targetClass == JSONObject.class) {
        return (T) JSONObject.parseObject((String) source);
      } else {
        return JSONObject.parseObject((String) source, targetClass);
      }
    }

    // JSONObject.parseObject暂不支持object，待温少增加相应API
    String jsonString = JSON.toJSONString(source);
    return JSONObject.parseObject(jsonString, targetClass);
  }

  /**
   * toEntity:将对象转换为装有指定类型实体的List
   *
   * @return T
   */
  public static <T> List<T> toList(Object source, Class<T> targetClass) {
    ArrayList<T> list = new ArrayList<T>();
    if (source == null || "".equals(source.toString())) {
      return list;
    }
    if (source.getClass() == String.class) {
      return JSONArray.parseArray((String) source, targetClass);
    }
    if (source.getClass() == JSONArray.class) {
      JSONArray array = (JSONArray) source;
      for (int i = 0; i < array.size(); i++) {
        //                System.out.println(array.get(i));
      }
    }
    // 待温少增加相应API
    String jsonString = JSON.toJSONString(source);
    return JSONArray.parseArray(jsonString, targetClass);
  }

  @SuppressWarnings("rawtypes")
  public static boolean isSimpleType(Class clazz) {
    return simpleTypeSet.contains(clazz);
  }

  public static <T> T parseObject(String source, Class<T> clazz) {
    if (isSimpleType(clazz)) {
      Parser parser = Parser.PARSER_MAP.get(clazz);
      if (parser == null) {
        throw new RuntimeException("暂不支持该类型的字符串值转换为" + clazz.getSimpleName());
      }
      Object result = parser.parse(source);
      return (T) result;
    }
    // 非简单类型的数据，按指定clazz转化

    return (T) JSONObject.parseObject(source, clazz);
  }

  @SuppressWarnings("rawtypes")
  public static Object toObject(String source, Class clazz) {
    if (isSimpleType(clazz)) {
      Parser parser = Parser.PARSER_MAP.get(clazz);
      if (parser == null) {
        throw new RuntimeException("暂不支持该类型的字符串值转换为" + clazz.getSimpleName());
      }
      Object result = parser.parse(source);
      return result;
    }
    // 非简单类型的数据，按指定clazz转化
    return JSONObject.parseObject(source, clazz);
  }

  /**
   * 只负责拷贝简单类型属性 被构建对象,必须包含空参数的构造函数
   */
  public static <T> T newFromJSON(Class<T> clazz, JSONObject json) throws Exception {
    T result = clazz.newInstance();
    Method[] methods = clazz.getMethods();
    for (Method method : methods) {
      Class<?>[] classes = method.getParameterTypes();
      if (method.getName().startsWith("set") && classes.length == 1 && JSONUtil
          .isSimpleType(classes[0])) {
        try {
          Object value = JSONUtil.toObject(json.getString(getKey(method.getName())), classes[0]);
          method.invoke(result, value);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return result;
  }

  private static String getKey(String setterMethodName) {
    return setterMethodName.substring(3, 4).toLowerCase() + setterMethodName.substring(4);
  }
}

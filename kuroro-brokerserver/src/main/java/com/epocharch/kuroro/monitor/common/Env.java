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

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.web.context.ContextLoader;

/**
 * 2010-12-6 User: heng.dong
 */
@SuppressWarnings("unchecked")
public class Env {

  public static final String APPCONTEXT_FILENAME = "applicationContext.xml";
  static final Env INSTANCE = new Env();
  static final String separator = File.separator;
  private static final Map<Class<?>, Method[]> methodsMap = new HashMap<Class<?>, Method[]>();
  private static final Map<String, Method> serviceMethodMap = new HashMap<String, Method>();
  private static ApplicationContext applicationContext;//

  private Env() {

  }

  /**
   * getApplicationContext
   *
   * @return ApplicationContext
   */
  public static ApplicationContext getApplicationContext() {
    if (applicationContext != null) {
      return applicationContext;
    }
    if (ContextLoader.getCurrentWebApplicationContext() != null) {
      applicationContext = ContextLoader.getCurrentWebApplicationContext();
    }

    if (applicationContext == null) {
      newENV();
    }
    return applicationContext;
  }

  public static void setApplicationContext(ApplicationContext context) {
    applicationContext = context;
  }

  public static Env newENV() {
    applicationContext = new ClassPathXmlApplicationContext(APPCONTEXT_FILENAME);
    return INSTANCE;
  }

  public static ApplicationContext newApplicationContext() {
    applicationContext = new ClassPathXmlApplicationContext(APPCONTEXT_FILENAME);
    return applicationContext;
  }

  /**
   * 根据bean id获取bean
   */
  public static Object getBean(String name) {
    return getApplicationContext().getBean(name);
  }

  /**
   * 根据bean id获取指定类型的bean
   */

  public static <T> T getBean(String name, Class<T> clazz) {
    return (T) getBean(name);
  }

  public static boolean containsBean(String name) {
    return getApplicationContext().containsBean(name);
  }

  public static Object invoke(String serviceId, String param) throws Exception {
    String[] array = null;
    if (serviceId == null || (array = serviceId.split("\\.")).length != 2) {
      throw new RuntimeException("不合法的serviceId，请检查格式是否为:beanId.methodName形式");
    }
    // 取得method对象
    String beanId = array[0];
    Object serviceBean = Env.getBean(beanId);
    Method method = getServiceMethod(serviceBean, serviceId);
    int paramCount = method.getParameterTypes().length;
    // 参数以JSONArray的形式存放
    if (paramCount > 1) {
      throw new RuntimeException("ajax service bean method params count must be 0 or 1.");
    }

    Object[] params = new Object[paramCount];
    for (int i = 0; i < paramCount; i++) {
      params[i] = JSONUtil.parseObject(param, method.getParameterTypes()[i]);
    }
    return method.invoke(serviceBean, params);
  }

  public static Method getServiceMethod(Object bean, String serviceId) {
    if (serviceMethodMap.containsKey(serviceId)) {
      return serviceMethodMap.get(serviceId);
    }
    String methodName = serviceId.split("\\.")[1];
    Class<? extends Object> clazz = bean.getClass();
    Method[] methods = null;
    if (methodsMap.containsKey(clazz)) {
      methods = methodsMap.get(clazz);
    } else {
      methods = clazz.getMethods();
      methodsMap.put(clazz, methods);
    }

    Set<Method> mSet = new HashSet<Method>();
    Method result = null;
    for (Method method : methods) {
      if (method.getName().equals(methodName)) {
        mSet.add(method);
        result = method;
      }
    }
    if (mSet.size() == 0) {
      throw new RuntimeException("找不到bean：" + bean.getClass() + "中的方法：：" + methodName);
    }
    // 服务层方法中，最好不要出现重载的服务方法
    if (mSet.size() > 1) {
      throw new RuntimeException("bean：" + bean.getClass() + "中存在多个方法：" + methodName);
    }
    serviceMethodMap.put(serviceId, result);
    return result;
  }
}

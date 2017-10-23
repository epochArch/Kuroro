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

package com.epocharch.kuroro.monitor.support;

import com.alibaba.fastjson.JSONObject;
import com.epocharch.kuroro.monitor.common.Env;
import com.epocharch.kuroro.monitor.common.JSONUtil;
import com.epocharch.kuroro.monitor.common.WebUtils;
import com.epocharch.kuroro.monitor.service.DataRouterService;
import com.epocharch.kuroro.monitor.support.switcher.SensitiveSwitcher;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 用处理ajax请求 2012-10-18
 *
 * @author dongheng
 */
public class AjaxServlet extends HttpServlet {

  static final String ERROR_RESULT = "{e:#e#}";
  static final Map<Class<?>, Method[]> methodsMap = new HashMap<Class<?>, Method[]>();
  static final Map<String, Method> serviceMethodMap = new HashMap<String, Method>();
  private static final long serialVersionUID = 1L;
  private static final Log logger = LogFactory.getLog(AjaxServlet.class);
  private static SensitiveSwitcher sensitiveService;

  /**
   * Constructor of the object.
   */
  public AjaxServlet() {
    super();
    sensitiveService = new SensitiveSwitcher();
    sensitiveService.regist();
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

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  public void init() throws ServletException {
    super.init();
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json;charset=UTF-8");// 返回的xml文件
    boolean sensitive = false;
    HttpServletRequest req = (HttpServletRequest) request;
    Object obj = WebUtils.getLoginUser(req);
    String rmi = request.getParameter("rmi");

    String result = ERROR_RESULT;
    String username = "";
    if (rmi != null) {
      try {
        AjaxRequest ajaxRequest = JSONUtil.parseObject(rmi, AjaxRequest.class);
        //默认南汇机房
        if (StringUtils.isEmpty(ajaxRequest.getI())) {
          ajaxRequest.setI(MonitorZkUtil.localIDCName);
        }

        if (sensitiveService.contains(ajaxRequest.getServiceId())) {
        } else {
          sensitive = false;
        }
        Object object = DataRouterService.route(ajaxRequest);
        if (object != null && JSONUtil.isSimpleType(object.getClass())) {
          response.getWriter().print(object);
          if (sensitive) {
            logger.info("User:" + username + "rmi:" + rmi + "result:" + object);
          }
          return;
        }
        result = JSONObject.toJSONString(object);
        if (result != null) {
          response.getWriter().print(result);
          if (sensitive) {
            logger.info("User:" + username + "rmi:" + rmi + "result:" + object);
          }
        }
        return;
      } catch (Exception e) {
        e.printStackTrace();
        String message = ExceptionUtils.getFullStackTrace(e);
        response.getWriter().print(message);
        if (sensitive) {
          logger.info("User:" + username + "rmi:" + rmi + "result:" + message);
        }
        return;
      }
    }
    response.getWriter().print(result);
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    doGet(request, response);
  }

}
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

package com.epocharch.kuroro.monitor.log.aop;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.epocharch.kuroro.monitor.common.JSONUtil;
import com.epocharch.kuroro.monitor.common.WebUtils;
import com.epocharch.kuroro.monitor.dao.BaseMybatisDAO;
import com.epocharch.kuroro.monitor.dto.User;
import com.epocharch.kuroro.monitor.intelligent.impl.AnalystService;
import com.epocharch.kuroro.monitor.support.AjaxRequest;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Aspect
public class OperationLogAspect {

  private static final Logger logger = Logger.getLogger(OperationLogAspect.class);

  @Autowired
  private AnalystService analystService;

  private BaseMybatisDAO baseMybatisDAO;

  @Pointcut(value = "@annotation(OperationLogAnnotation)")
  public void annotationMatch() {
  }

  @Pointcut(value = "annotationMatch()")
  public void executeAction() {
  }

  @AfterReturning(pointcut = "executeAction()", argNames = "joinPoint,rtv", returning = "rtv")
  public void afterAspect(JoinPoint joinPoint,
      Object rtv) {
    //获取结果状态码(0:成功,1:失败)
    String resultCode = getResultCode(rtv);
    executeAspect(joinPoint, resultCode, null);
  }

  @AfterThrowing(throwing = "ex", pointcut = "executeAction()")
  public void throwsAspect(JoinPoint joinPoint, Throwable ex) {
    //抛出异常时执行该逻辑，说明操作失败
    String resultCode = WebUtils.RESULT_CODE_FAILURE_VALUE;
    executeAspect(joinPoint, resultCode, ex);
  }

  private void executeAspect(JoinPoint joinPoint, String resultCode, Throwable ex) {
    if (joinPoint.getArgs() == null) {
      return;
    }
    //从spring容器获取request对象,可以直接注解获得
    HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder
        .getRequestAttributes()).getRequest();
    //获取idc信息
    String idcName = getIdcName(request);
    //操作者名字
    String userName = getUserName(request);
    //功能模块名和操作名
    Map<String, String> map = getModuleNameAndOperationName(joinPoint);
    String moduleName = map.get("moduleName");
    String operationName = map.get("operationName");
    //操作者IP地址
    String ip = analystService.getLocalIP();
    //获取操作内容
    //方法签名
    MethodSignature ms = (MethodSignature) joinPoint.getSignature();
    //方法对象
    Method method = ms.getMethod();
    //获取方法名
    String methodName = method.getName();
    String operationDetails = getOperationDetails(joinPoint.getArgs(), methodName, ex);
    //创建日志对象
    OperationLog operationLog = new OperationLog();

    operationLog.setIdcName(idcName);
    operationLog.setUserName(userName);
    operationLog.setOperationTime(new Date());//操作时间
    operationLog.setOperationDetails(operationDetails);//操作内容
    operationLog.setIp(ip);//操作者IP地址
    operationLog.setModuleName(moduleName);//操作功能名
    operationLog.setOperationName(operationName);//操作名
    operationLog.setResultCode(resultCode);//0 ： 成功， 1 ：失败
    logger.info(
        userName + " operates module " + moduleName + ", and execute method " + operationName);
    baseMybatisDAO.insert("monitor_operation_log.insert", operationLog);
  }

  private String getResultCode(Object rtv) {
    String resultCode = "";
    if (rtv instanceof Map) {
      Map<String, String> rtvMap = (Map<String, String>) rtv;
      resultCode = rtvMap.get("resultCode");
    } else {
      //other
    }
    if (StringUtils.isEmpty(resultCode)) {
      resultCode = WebUtils.RESULT_CODE_FAILURE_VALUE;
    }
    return resultCode;
  }

  private String getIdcName(HttpServletRequest request) {
    //获取zone信息
    String rmi = request.getParameter("rmi");
    AjaxRequest ajaxRequest = JSONUtil.parseObject(rmi, AjaxRequest.class);
    String idcName = ajaxRequest.getI();
    //默认南汇机房(ZONE_NH)
    if (StringUtils.isEmpty(idcName)) {
      idcName = MonitorZkUtil.localIDCName;
    }
    return idcName;
  }

  private String getUserName(HttpServletRequest request) {
    String userName = "";
    //获取登陆对象
    Object obj = WebUtils.getLoginUser(request);
    if (obj != null) {
      User user = (User) obj;
      userName = user.getName();
    }
    if (StringUtils.isEmpty(userName)) {
      userName = WebUtils.UNKNOWN_USER;
    }
    return userName;
  }

  /*
   * 获取方法上的注解信息
   */
  private Map<String, String> getModuleNameAndOperationName(JoinPoint joinPoint) {
    Map<String, String> map = new HashMap<String, String>();
    Class targetClass = null;
    String targetName = joinPoint.getTarget().getClass().getName();
    String methodName = joinPoint.getSignature().getName();
    Object[] arguments = joinPoint.getArgs();
    try {
      targetClass = Class.forName(targetName);
      Method[] methods = targetClass.getMethods();
      for (Method method : methods) {
        if (method.getName().equals(methodName)) {
          Class[] clazzs = method.getParameterTypes();
          if (clazzs.length == arguments.length) {
            String moduleName = method.getAnnotation(OperationLogAnnotation.class).moduleName();
            String operationName = method.getAnnotation(OperationLogAnnotation.class)
                .operationName();
            map.put("moduleName", moduleName);
            map.put("operationName", operationName);
            break;
          }
        }
      }
    } catch (ClassNotFoundException e) {
      logger.error("get the target method class by its string name error.");
      e.printStackTrace();
    }
    return map;
  }

  /*
   * 获取操作内容
   */
  public String getOperationDetails(Object[] args, String methodName, Throwable ex) {
    if (args == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[方法名:" + methodName + "]");
    if (ex != null) {
      String message = ExceptionUtils.getFullStackTrace(ex);
      sb.append("[异常信息:" + message + "]");
    }
    //遍历参数对象
    for (Object arg : args) {
      if (arg instanceof Map) {
        Map<String, Object> map = (Map<String, Object>) arg;
        handleArgs(map, sb);
      } else if (arg instanceof String) {
        String jsonArg = (String) arg;
        JSONObject jsonObj = JSON.parseObject(jsonArg);
        Map<String, Object> map = (Map<String, Object>) jsonObj;
        handleArgs(map, sb);
      }
    }
    return sb.toString();
  }

  private String handleArgs(Map<String, Object> map, StringBuilder sb) {
    Iterator<String> iter = map.keySet().iterator();
    while (iter.hasNext()) {
      String key = iter.next();
      Object value = map.get(key);
      sb.append("[" + key + ":" + value + "]");
      System.out.println("key:" + key + ",value:" + value);
    }
    return sb.toString();
  }

  public void setBaseIbaitsDAO(BaseMybatisDAO baseIbaitsDAO) {
    this.baseMybatisDAO = baseIbaitsDAO;
  }
}

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
import com.epocharch.kuroro.monitor.common.JSONUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang.StringUtils;

/**
 * 初始化监控任务
 *
 * @author dongheng
 */
public class InitAnalyseJob extends HttpServlet implements Servlet {

  private static final long serialVersionUID = 3248449177229197158L;

  public static Set<String> getKeysByValue(Map<String, Integer> map, int value) {
    Set<String> set = new HashSet<String>();
    for (Entry<String, Integer> entry : map.entrySet()) {
      if (entry.getValue().intValue() == value) {
        set.add(entry.getKey());
      }
    }
    return set;
  }

  @Override
  public void init() throws ServletException {
    super.init();
  }

  /**
   * 1.m=add|delete|&app=''
   * <p>
   * 2.m=master&ip=''
   * <p>
   * 3.m=bak&ip=''
   */
  @SuppressWarnings("unchecked")
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String switchName = request.getParameter("switch");
    if (StringUtils.isEmpty(switchName)) {
      response.getWriter()
          .println("info: "
              + "params switch is not null,values is option from cal|jmx|logPath|logSendmaster|print");
    }
    BaseSwitcher ds = BaseSwitcher.getBaseSwitcher(switchName);
    Object obj = null;
    if (ds != null) {
      Map<String, String[]> params = (Map<String, String[]>) request.getParameterMap();
      obj = ds.logic(params);
    } else {
      JSONObject json = new JSONObject();
      json.put("message",
          "switch is unvaliable,option value is cal,master,logPath,logSend,jmx,print");
      obj = json;
    }
    try {
      Map<String, Object> map = new HashMap<String, Object>();
      if (JSONUtil.isSimpleType(obj.getClass())) {
        map.put("JSON", obj);
      } else {
        map.put("JSON", JSONObject.toJSONString(obj));
      }
      response.getWriter().println(JSONObject.toJSONString(obj));
    } finally {
      response.getWriter().close();
    }
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    doGet(request, response);
  }
}

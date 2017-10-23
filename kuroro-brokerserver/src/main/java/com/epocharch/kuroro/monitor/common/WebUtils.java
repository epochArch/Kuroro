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

import com.epocharch.kuroro.common.constants.Constants;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

/**
 * @author hello_yun
 */
public class WebUtils {

  public static final String PRODUCTION = Constants.PRODUCTION.replace("/", "").trim()
      .toLowerCase();
  public final static long ROLE_ASSOCIATION_TYPE_MENU = 1;
  //默认权限
  public static final String DEFAULT_ROLE = "kuroro_normal";
  public static final String SUPER_USER = "super_admin";
  public static final String SUPER_USER_PASSWORD = "kuroro_monitor";
  public static final String GUEST_USER = "guest";
  //系统操作时，成功和失败的状态码
  public static final String RESULT_CODE_KEY = "resultCode";
  public static final String RESULT_CODE_SUCCESS_VALUE = "0";
  public static final String RESULT_CODE_FAILURE_VALUE = "1";
  public static final String UNKNOWN_USER = "Unknown User";
  public static Map<String, String> urlMapping = new HashMap<String, String>();

  static {
    urlMapping.put("usrMgr", "");
    urlMapping.put("roleMgr", "");
  }

  public static String getUrlByCode(String code) {
    return urlMapping.get(code);
  }

  /**
   * @return 返回登录用户
   */
  public static Object getLoginUser(HttpServletRequest request) {
    return request.getSession().getAttribute("user");
  }
}

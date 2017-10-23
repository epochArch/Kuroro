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

import com.alibaba.fastjson.JSONObject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

/**
 * @author dongheng
 */
public class ExceptionInfoUtil {

  private static Map<String, String> errorContainer = new HashMap<String, String>();

  public static String getExceptionLogs(String eKey, Throwable e) {
    StringWriter out = new StringWriter();
    try {
      e.printStackTrace(new PrintWriter(out));
      String errorInfo = out.toString();
      errorContainer.put(eKey, errorInfo);
      return errorInfo;
    } catch (Exception e2) {

    } finally {
      IOUtils.closeQuietly(out);
    }
    return StringUtils.EMPTY;
  }

  public static void putExceptionLogs(String eKey, Throwable e) {
    StringWriter out = new StringWriter();
    try {
      e.printStackTrace(new PrintWriter(out));
      String errorInfo = out.toString();
      errorContainer.put(eKey, errorInfo);
    } catch (Exception e2) {

    } finally {
      IOUtils.closeQuietly(out);
    }
  }

  public synchronized static String getNotifyContent() {
    if (errorContainer.size() > 0) {
      String content = JSONObject.toJSONString(errorContainer);
      errorContainer.clear();
      return content;
    }
    return StringUtils.EMPTY;
  }
}

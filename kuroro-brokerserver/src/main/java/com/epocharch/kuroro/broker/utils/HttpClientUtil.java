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

package com.epocharch.kuroro.broker.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);

  public static String postDoPostURL(String url, Map<String, String> params) {
    HttpClient httpClient = new HttpClient();
    // 创建POST方法的 实例
    PostMethod postMethod = new PostMethod(url);
    postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 2000);
    if (params != null && params.size() > 0) {
      NameValuePair[] data = new NameValuePair[params.keySet().size()];
      Iterator<Entry<String, String>> it = params.entrySet().iterator();
      int i = 0;
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        Object key = entry.getKey();
        Object value = entry.getValue();
        data[i] = new NameValuePair(key.toString(), value.toString());
        i++;
      }
      // 将表单的 值放入postMethod中
      postMethod.setRequestBody(data);
      postMethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "UTF-8");
    }
    String response = null;
    try {
      // 执行postMethod,并取得状态码
      int statusCode = httpClient.executeMethod(postMethod);
      response = new String(postMethod.getResponseBodyAsString().getBytes(), "UTF-8");
      if (statusCode != 200) {
        response = null;
        LOGGER.error(url + "->Method failed: " + postMethod.getStatusLine());
      }
    } catch (Exception e) {
      response = null;
      LOGGER.error(url + "->" + e.getMessage(), e);
    } finally {
      postMethod.releaseConnection();

    }
    return response;
  }
}

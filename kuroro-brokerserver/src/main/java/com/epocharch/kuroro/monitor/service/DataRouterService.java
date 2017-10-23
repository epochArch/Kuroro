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

package com.epocharch.kuroro.monitor.service;

import com.alibaba.fastjson.JSONObject;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.monitor.common.Env;
import com.epocharch.kuroro.monitor.common.ExceptionInfoUtil;
import com.epocharch.kuroro.monitor.common.MonitorHttpClientUtil;
import com.epocharch.kuroro.monitor.support.AjaxRequest;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 提供数据路由服务
 *
 * @author dongheng
 */
public class DataRouterService {

  /**
   * 内部逻辑处理
   */
  public static Object route(AjaxRequest ajaxRequest) throws RuntimeException {
    JSONObject json = new JSONObject();
    try {
      if (isLocalIdc(ajaxRequest.getI()) || StringUtils.isEmpty(ajaxRequest.getI())) {
        return Env.invoke(ajaxRequest.getServiceId(), ajaxRequest.getP());
      } else {
        RetryCounter retry = new RetryCounter(3, 100, TimeUnit.MILLISECONDS);
        String url = KuroroUtil.currentOpenAPI(ajaxRequest.getI());
        while (retry.shouldRetry()) {
          try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("rmi", JSONObject.toJSONString(ajaxRequest));
            return MonitorHttpClientUtil.remoteExcuter(url, params);
          } catch (Exception e) {
            e.printStackTrace();
            ExceptionInfoUtil.putExceptionLogs("DataRouterService error", e);
            retry.useRetry();
            retry.sleepUntilNextRetry();
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      json.put("-1", ExceptionUtils.getFullStackTrace(e));
    }
    return json;
  }

  public static boolean isLocalIdc(String idcName) {
    return StringUtils.equals(idcName, MonitorZkUtil.localIDCName);
  }

  public static class RetryCounter {

    private static final Log LOG = LogFactory.getLog(RetryCounter.class);
    private final int maxRetries;
    private final int retryIntervalMillis;
    private final TimeUnit timeUnit;
    private int retriesRemaining;

    public RetryCounter(int maxRetries, int retryIntervalMillis, TimeUnit timeUnit) {
      this.maxRetries = maxRetries;
      this.retriesRemaining = maxRetries;
      this.retryIntervalMillis = retryIntervalMillis;
      this.timeUnit = timeUnit;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public void sleepUntilNextRetry() throws InterruptedException {
      int attempts = getAttemptTimes();
      long sleepTime = (long) (retryIntervalMillis * Math.pow(2, attempts));
      LOG.info("The " + attempts + " times to retry  after sleeping " + sleepTime + " ms");
      timeUnit.sleep(sleepTime);
    }

    public boolean shouldRetry() {
      return retriesRemaining > 0;
    }

    public void useRetry() {
      retriesRemaining--;
    }

    public int getAttemptTimes() {
      return maxRetries - retriesRemaining + 1;
    }

  }
}

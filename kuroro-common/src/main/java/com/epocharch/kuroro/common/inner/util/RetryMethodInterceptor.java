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

package com.epocharch.kuroro.common.inner.util;

import java.lang.reflect.Method;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetryMethodInterceptor implements MethodInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(RetryMethodInterceptor.class);
  private long retryIntervalWhenException;
  private Object target;
  /**
   * 指定异常的Class
   */
  @SuppressWarnings("rawtypes")
  private Class clazz;

  @SuppressWarnings("rawtypes")
  public RetryMethodInterceptor(Object target, long retryIntervalWhenException, Class clazz) {
    this.target = target;
    this.retryIntervalWhenException = retryIntervalWhenException;
    this.clazz = clazz;
  }

  @Override
  public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
      throws Throwable {
    while (true) {
      try {
        return proxy.invoke(this.target, args);
      } catch (Exception e) {
        // 判断异常类型,如果是指定的异常或异常子类，则retry
        if (clazz.isInstance(e)) {
          LOG.error("Error in Proxy of " + this.target + ", wait " + retryIntervalWhenException
              + "ms before retry. ", e);
          Thread.sleep(retryIntervalWhenException);
        } else {
          throw e;
        }
      }
    }
  }

}

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

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 * Created by zhoufeiqiang on 16/10/2017.
 */
public class LoadPropertyConfigurer extends PropertyPlaceholderConfigurer implements
    InitializingBean {

  private static String poolId;

  public static String getPoolId() {
    return poolId;
  }

  public void setPoolId(String poolId) {
    this.poolId = poolId;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    if (KuroroUtil.isBlankString(poolId)) {
      throw new RuntimeException("appId should not be blank");
    }
  }

}

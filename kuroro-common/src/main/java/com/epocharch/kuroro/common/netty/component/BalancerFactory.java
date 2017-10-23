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

package com.epocharch.kuroro.common.netty.component;

import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import java.util.HashMap;
import java.util.Map;

public class BalancerFactory {

  private static BalancerFactory factory = new BalancerFactory();

  private static Map<String, String> balancerContainer;

  private BalancerFactory() {
    super();
    balancerContainer = new HashMap<String, String>();
    balancerContainer
        .put(InternalPropKey.BALANCER_NAME_ROUNDROBIN, RoundRobinBalancer.class.getName());
    balancerContainer
        .put(InternalPropKey.BALANCER_NAME_CONSISTENTHASH, ConsistentHashBalancer.class.getName());
  }

  public static BalancerFactory getInstance() {
    return factory;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public LoadBalancer<HostInfo> getBalancer(String name) throws Exception {
    if (KuroroUtil.isBlankString(name)) {
      throw new NullPointerException("Balancer name must not null");
    }
    String clazzName = balancerContainer.get(name);
    if (clazzName != null) {
      try {
        Class clazz = Class.forName(clazzName);
        return (LoadBalancer<HostInfo>) clazz.newInstance();
      } catch (Throwable e) {
        throw new NullPointerException("Can't find " + clazzName + " balancer");
      }
    } else {
      throw new NullPointerException("Can't find " + name + " balancer");
    }
  }

}

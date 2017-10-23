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

package com.epocharch.kuroro.common.constants;

/**
 * Created by zhoufeiqiang on 10/10/2017.
 */
public class InternalPropKey {

  public static final String ZONE_IDCNAME = "kuroro.zone.idc.name";
  public static final String ZONE_LOCALZONE = "kuroro.zone.local.name";
  public static final String ZONE_KURORO_ZK_ROOT_PATH = "kuroro.zone.zk.root.path";
  public static final String IDC_KURORO_ZK_ROOT_PATH = "kuroro.idc.zk.root.path";
  public static final String IDC_OPEN_API_NAME = "kuroro.openapi.root";
  public static final String POOL_LEVEL = "pool.level";
  public static final String POOIDNAME = "poolId";
  public static final String BALANCER_NAME_ROUNDROBIN = "RoundRobin";
  public static final String BALANCER_NAME_CONDITIONSELECTION = "ConditionSelection";
  public static final String BALANCER_NAME_CONSISTENTHASH = "ConsistentHash";

  /**
   * appliaction run environment。divided into three level：
   * test, staging, production.
   * default value test
   */
  public static final String CURRENT_ENV = "app.current.env";
  public static final String JMX_DOMAIN_NAME = "com.epocharch.kuroro";
  public static final String ADMIN_NAME = "admin";
  public static final String ADMIN_PASS = "admin";
  public static final String MONGO_USERNAME = "userName";
  public static final String MONGO_PASSWORD = "passWord";
}

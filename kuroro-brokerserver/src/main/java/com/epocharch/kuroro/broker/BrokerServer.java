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

package com.epocharch.kuroro.broker;

import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.zkclient.ZkClient;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhoufeiqiang on 25/04/2017.
 */
public class BrokerServer {

  protected int reTryTime = 10;

  protected Lock lock = new ReentrantLock();

  protected HostInfo hi = null;

  protected String host = null;

  protected ZkClient idcZkClient = null;

  protected ZkClient zoneZkClient = null;

  protected Set<String> zonesNameSet = null;

  protected void watchZoneChange() {
  }


  protected void watchServerZkIp(final ZkClient localZk, String parentPath,
      final String childPath) {
  }


}

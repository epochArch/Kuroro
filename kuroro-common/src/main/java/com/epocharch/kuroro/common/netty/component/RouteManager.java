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
import com.epocharch.kuroro.common.inner.config.Operation;
import com.epocharch.kuroro.common.inner.config.impl.ExtPropertiesChange;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RouteManager {

  private static final Logger LOG = LoggerFactory.getLogger(RouteManager.class);
  private Map<String, HostInfo> hostInfos = new ConcurrentHashMap<String, HostInfo>();
  private ZkClient zkClient = KuroroZkUtil.initIDCZk();
  private LoadBalancer<HostInfo> balancer;
  private boolean initialized = false;
  private List<ExtPropertiesChange> changeList = new ArrayList<ExtPropertiesChange>();
  private String parentPath;

  public RouteManager(String parentPath) {
    try {
      this.parentPath = parentPath;
      balancer = BalancerFactory.getInstance()
          .getBalancer(InternalPropKey.BALANCER_NAME_ROUNDROBIN);
      observeChild(parentPath);
      List<String> ls = zkClient.getChildren(parentPath);
      observeChildData(parentPath, ls);
      balancer.updateProfiles(hostInfos.values());
      initialized = true;

    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  public RouteManager(String parentPath, String balancerName) {
    try {
      this.parentPath = parentPath;
      balancer = BalancerFactory.getInstance().getBalancer(balancerName);
      observeChild(parentPath);
      List<String> ls = zkClient.getChildren(parentPath);
      observeChildData(parentPath, ls);
      balancer.updateProfiles(hostInfos.values());
      initialized = true;

    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  private void observeChildData(final String parentPath, List<String> childList) {
    if (childList != null && childList.size() > 0) {
      for (String child : childList) {
        String childPath = KuroroUtil.getChildFullPath(parentPath, child);
        if (zkClient.exists(childPath)) {
          HostInfo hi = zkClient.readData(childPath);
          hostInfos.put(child, hi);
          observeSpecifyChildData(childPath);
        }
      }
    }
  }

  private void observeChild(String basePath) {
    if (zkClient.exists(basePath)) {
      zkClient.subscribeChildChanges(basePath, new IZkChildListener() {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds)
            throws Exception {
          if (parentPath != null) {
            Map<String, HostInfo> newHostInfos = new HashMap<String, HostInfo>();
            HostInfo hi = null;
            String childPath;
            for (String child : currentChilds) {
              childPath = KuroroUtil.getChildFullPath(parentPath, child);
              if (!hostInfos.containsKey(child)) {
                hi = zkClient.readData(childPath, true);
                observeSpecifyChildData(childPath);
              } else {
                hi = hostInfos.get(child);
              }
              if (hi != null) {
                newHostInfos.put(childPath, hi);
              }
            }
            hostInfos = newHostInfos;
            // route update
            balancer.updateProfiles(hostInfos.values());
          }
        }
      });
    }

  }

  private void observeSpecifyChildData(String childPath) {
    zkClient.subscribeDataChanges(childPath, new IZkDataListener() {
      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
        if (!KuroroUtil.isBlankString(dataPath)) {
          String child = KuroroUtil.getChildShortPath(dataPath);
          if (hostInfos.containsKey(child)) {
            // delete onchange
            addOnChange(child, hostInfos.get(child), Operation.DELETE);
            hostInfos.remove(child);
          }
        }
        balancer.updateProfiles(hostInfos.values());

      }

      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
        if (!KuroroUtil.isBlankString(dataPath)) {
          String child = KuroroUtil.getChildShortPath(dataPath);
          addHostInfo(child, data);
          // update onchange
          addOnChange(child, data, Operation.MODIFY);
          balancer.updateProfiles(hostInfos.values());
        }
      }
    });
  }

  private void addHostInfo(String child, Object obj) {
    if (obj != null) {
      HostInfo sp = (HostInfo) obj;
      hostInfos.put(child, sp);
    }
  }

  public Collection<HostInfo> getAllHostInfo() {
    return hostInfos.values();
  }

  public HostInfo route() {
    HostInfo hi = null;
    while (!initialized) {
      Thread.yield();
    }
    hi = balancer.select();
    if (hi == null) {
      updateBrokerServerIp();
      hi = balancer.select();
    }
    return hi;
  }

  private void addOnChange(String key, Object value, Operation oper) {
    if (changeList != null) {
      for (ExtPropertiesChange change : changeList) {
        change.onChange(key, value, oper);
      }
    }

  }

  public void addChange(ExtPropertiesChange change) {
    if (change == null) {
      throw new IllegalArgumentException();
    }
    this.changeList.add(change);
  }

  private void updateBrokerServerIp() {
    if (!KuroroUtil.isBlankString(parentPath) && hostInfos.isEmpty()) {

      List<String> ls = zkClient.getChildren(parentPath);
      for (String child : ls) {
        String childPath = KuroroUtil.getChildFullPath(parentPath, child);
        if (zkClient.exists(childPath)) {
          HostInfo hi = zkClient.readData(childPath);
          hostInfos.put(child, hi);
        }
      }

      if (!hostInfos.isEmpty()) {
        balancer.updateProfiles(hostInfos.values());
      }
    }
  }

}

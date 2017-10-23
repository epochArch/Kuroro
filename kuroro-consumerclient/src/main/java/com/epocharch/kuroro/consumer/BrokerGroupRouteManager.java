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

package com.epocharch.kuroro.consumer;

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.config.Operation;
import com.epocharch.kuroro.common.inner.config.impl.ExtPropertiesChange;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.netty.component.BalancerFactory;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.netty.component.LoadBalancer;
import com.epocharch.kuroro.common.netty.component.RouteManager;
import com.epocharch.kuroro.consumer.impl.ConsumerFactoryImpl;
import com.epocharch.kuroro.consumer.impl.inner.ConsumerImpl;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerGroupRouteManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(RouteManager.class);
  private Map<String, HostInfo> hostInfos = new ConcurrentHashMap<String, HostInfo>();
  private ZkClient zkClient = null;
  private LoadBalancer<HostInfo> balancer;
  private boolean initialized = false;
  private String topicName = null;
  private String groupContext = null;
  private List<ExtPropertiesChange> changeList = new ArrayList<ExtPropertiesChange>();
  private String zoneZkRoot = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH) +
      Constants.SEPARATOR + System.getProperty(InternalPropKey.ZONE_LOCALZONE);
  private String idcZkroot = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH);
  private boolean isProduction = Boolean
      .parseBoolean(System.getProperty(InternalPropKey.POOL_LEVEL));
  private String brokerGroupPath;


  //默认负载均衡
  public BrokerGroupRouteManager(String brokerGroupPath, String topicName, String groupContext) {
    try {
      this.groupContext = groupContext;
      this.topicName = topicName;
      this.brokerGroupPath = brokerGroupPath;
      balancer = BalancerFactory.getInstance().getBalancer(
          InternalPropKey.BALANCER_NAME_ROUNDROBIN);
      zkPathJudge();
      observeChild(brokerGroupPath);
      List<String> ls = zkClient.getChildren(brokerGroupPath);
      observeChildData(brokerGroupPath, ls);
      balancer.updateProfiles(hostInfos.values());
      initialized = true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  //指定负载均衡
  public BrokerGroupRouteManager(String brokerGroupPath, String topicName, String balancerName,
      String groupContext) {
    try {
      this.topicName = topicName;
      this.groupContext = groupContext;
      this.brokerGroupPath = brokerGroupPath;
      balancer = BalancerFactory.getInstance().getBalancer(balancerName);
      zkPathJudge();
      observeChild(brokerGroupPath);
      List<String> ls = zkClient.getChildren(brokerGroupPath);
      observeChildData(brokerGroupPath, ls);
      balancer.updateProfiles(hostInfos.values());
      initialized = true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private void observeChildData(final String parentPath,
      List<String> childList) {
    if (childList != null && childList.size() > 0) {
      for (String child : childList) {
        String childPath = KuroroUtil.getChildFullPath(parentPath,
            child);
        if (zkClient.exists(childPath)) {
          HostInfo hi = zkClient.readData(childPath);
          hostInfos.put(child, hi);
          observeSpecifyChildData(childPath);
        }
      }
    }
  }

  private void observeChild(String basePath) {
    System.out.println(basePath);
    if (zkClient.exists(basePath)) {
      zkClient.subscribeChildChanges(basePath, new IZkChildListener() {

        @Override
        public void handleChildChange(String parentPath,
            List<String> currentChilds) throws Exception {
          if (parentPath != null) {
            Map<String, HostInfo> newHostInfos = new HashMap<String, HostInfo>();
            HostInfo hi = null;
            String childPath;
            for (String child : currentChilds) {
              childPath = KuroroUtil.getChildFullPath(parentPath,
                  child);
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
            addOnChange(child, hostInfos.get(child),
                Operation.DELETE);
            hostInfos.remove(child);
          }
        }
        balancer.updateProfiles(hostInfos.values());
      }

      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {
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

  //Topic监听
  public void observeTopicData(String topicPath) {
    if (!zkClient.exists(topicPath)) {
      throw new NullPointerException("topicPath path is " + topicPath);
    } else {
      zkClient.subscribeDataChanges(topicPath, new IZkDataListener() {
        @Override
        public void handleDataDeleted(String topicPath) throws Exception {
        }

        @Override
        public void handleDataChange(String topicPath, Object data) throws Exception {
          TopicConfigDataMeta tb = (TopicConfigDataMeta) data;
          String brokerGroupPath = null;
          String _groupcontext = tb.getBrokerGroup();
          //groupContext为deteor上传过来的groupName｜｜_groupcontext为topic里的groupName内容
          if (groupContext != null && _groupcontext != null && groupContext.equals(_groupcontext)) {
            LOG.warn("group is null or group is not change! [groupContext={},_groupcontext={}]  "
                    + topicPath
                , groupContext, _groupcontext);
            return;
          } else {
            LOG.warn("group is changing! topicPath : " + topicPath, this);

            if (_groupcontext != null) {
              groupContext = _groupcontext;
            } else {
              _groupcontext = "Default";
              groupContext = _groupcontext;
              tb.setBrokerGroup(_groupcontext);
            }

            if (!KuroroUtil.isBlankString(idcZkroot) && isProduction) {
              brokerGroupPath = idcZkroot + Constants.ZONE_BROKER + _groupcontext
                  + Constants.ZONE_BROKER_CONSUMER;
            } else if (!KuroroUtil.isBlankString(zoneZkRoot) && !isProduction) {
              brokerGroupPath = zoneZkRoot + Constants.ZONE_BROKER + _groupcontext
                  + Constants.ZONE_BROKER_CONSUMER;
            }

            Map<String, Consumer> map = ConsumerFactoryImpl.getConsumerMap();
            Iterator<Map.Entry<String, Consumer>> it = map.entrySet().iterator();
            while (it.hasNext()) {

              Map.Entry<String, Consumer> entry = it.next();
              ConsumerImpl c = (ConsumerImpl) entry.getValue();
              try {
                if (c != null && c.getDest().getName().equals(tb.getTopicName().toString())) {
                  c.close();
                  LOG.warn("consumer is close! " + c.toString());
                } else {
                  continue;
                }

                BrokerGroupRouteManager routeManager = null;
                Map<String, BrokerGroupRouteManager> groupMap = ConsumerFactoryImpl.getGroupMap();
                if (groupMap.get(tb.getTopicName() + tb.getBrokerGroup()) != null) {
                  routeManager = groupMap.get(tb.getTopicName() + tb.getBrokerGroup());
                } else {
                  routeManager = new BrokerGroupRouteManager(brokerGroupPath, topicName,
                      _groupcontext);
                  groupMap.put(tb.getTopicName() + tb.getBrokerGroup(), routeManager);
                }
                c.setBrokerGroupRotueManager(routeManager);
                c.restart();
                it.remove();
                ConsumerFactoryImpl.consumerMap.put(c.toString(), c);
                LOG.warn("consumer is restart! " + c);
              } catch (Exception e) {
                LOG.error(c + " consumer change group is fail! " + e.getMessage());
              }
            }
          }
        }
      });
    }
  }

  //zone zk path and idc zk path
  private void zkPathJudge() {
    String topicPath = null;
    if (!KuroroUtil.isBlankString(idcZkroot) && isProduction) {
      topicPath = idcZkroot + Constants.ZONE_TOPIC + Constants.SEPARATOR + topicName;
    } else if (!KuroroUtil.isBlankString(zoneZkRoot) && !isProduction) {
      topicPath = zoneZkRoot + Constants.ZONE_TOPIC + Constants.SEPARATOR + topicName;
    }
    zkClient = KuroroZkUtil.initLocalMqZk();
    observeTopicData(topicPath);
  }


  private void updateBrokerServerIp() {
    if (!KuroroUtil.isBlankString(brokerGroupPath) && hostInfos.isEmpty()) {
      List<String> ls = zkClient.getChildren(brokerGroupPath);
      for (String child : ls) {
        String childPath = KuroroUtil.getChildFullPath(brokerGroupPath, child);
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

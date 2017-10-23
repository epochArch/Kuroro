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

package com.epocharch.kuroro.broker.producerserver.impl;

import com.alibaba.fastjson.JSONObject;
import com.epocharch.common.util.SystemUtil;
import com.epocharch.kuroro.broker.BrokerServer;
import com.epocharch.kuroro.broker.Config;
import com.epocharch.kuroro.broker.utils.MessageCountUtil;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.monitor.CloseMonitor;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.message.MessageLog;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.monitor.common.MonitorHttpClientUtil;
import com.epocharch.kuroro.monitor.intelligent.impl.AnalystService;
import com.epocharch.kuroro.monitor.support.AjaxRequest;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkStateListener;
import com.epocharch.zkclient.ZkClient;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.Watcher;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerServer extends BrokerServer {

  private static final Logger LOG = LoggerFactory.getLogger(ProducerServer.class);
  private static Map<String, AtomicLong> topicMapCount = new ConcurrentHashMap<String, AtomicLong>();
  private static Map<String, AtomicLong> topicMapSize = new ConcurrentHashMap<String, AtomicLong>();
  private final int workerCount = Integer
      .parseInt(System.getProperty("global.producerServer.workerCount", "0"));
  private final String brokerGroup = System
      .getProperty("brokerGroup", Constants.KURORO_DEFAULT_BROKER_GROUPNAME);
  private MessageDAO messageDAO;
  private AnalystService analystService;
  private MessageCountUtil messageCountUtil;
  //zone design
  private String zkRootPath = null;
  private String zoneIDCName = null;
  private String zoneProducerServer = null;
  private String idcZkProducerServerPath = null;

  public static Map<String, AtomicLong> getTopicMapCount() {
    return topicMapCount;
  }

  public static Map<String, AtomicLong> getTopicMapSize() {
    return topicMapSize;
  }

  public void setMessageDAO(MessageDAO messageDAO) {
    this.messageDAO = messageDAO;
  }

  public void setMessageCountUtil(MessageCountUtil messageCountUtil) {
    this.messageCountUtil = messageCountUtil;
  }

  public void start() throws Exception {

    zoneIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
    zkRootPath = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH);
    zoneProducerServer =
        Constants.ZONE_BROKER + Constants.SEPARATOR + brokerGroup + Constants.ZONE_BROKER_PRODUCER;
    idcZkProducerServerPath =
        System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH) + Constants.ZONE_BROKER
            + Constants.SEPARATOR + brokerGroup
            + Constants.ZONE_BROKER_PRODUCER;
    zonesNameSet = KuroroZkUtil.localIdcAllZones();

    //host = IPUtil.getFirstNoLoopbackIP4Address();
    host = SystemUtil.getLocalhostIp();
    hi = new HostInfo(host, Constants.KURORO_PRODUCER_PORT, 1);

    final ServerBootstrap bootstrap = workerCount == 0 ?
        new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(new DefaultThreadFactory("ProducerServer-BossExecutor")),
            Executors
                .newCachedThreadPool(new DefaultThreadFactory("ProducerServer-WorkerExecutor")))) :
        new ServerBootstrap(new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(new DefaultThreadFactory("ProducerServer-BossExecutor")),
            Executors
                .newCachedThreadPool(new DefaultThreadFactory("ProducerServer-WorkerExecutor")),
            workerCount));
    bootstrap.setPipelineFactory(new ProducerServerPipelineFactory(messageDAO));

    // 启用close monitor
    CloseMonitor closeMonitor = new CloseMonitor();
    int port = Integer.parseInt(System.getProperty("producerCloseMonitorPort", Constants.PRODUCER_CLOSE_MONITOR_PORT));
    closeMonitor.start(port, false, new CloseMonitor.CloseHook() {
      @Override
      public void onClose() {
        for (String zoneName : zonesNameSet) {
          zoneZkClient = KuroroZkUtil.initMqZK(zoneName);
          if (zoneZkClient == null) {
            continue;
          }
          try {
            LOG.info("Start to close producer server");
            String zoneProducerPath =
                zkRootPath + Constants.SEPARATOR + zoneName + zoneProducerServer
                    + Constants.SEPARATOR + host;

            if (zoneZkClient.exists(zoneProducerPath)) {
              zoneZkClient.delete(zoneProducerPath);
              LOG.info("Producer node[" + zoneProducerPath + "] deleted");
            }
            LOG.info("Check producer node: " + zoneZkClient.exists(zoneProducerPath));

            LOG.info("Sleep for a while to wait for producer clients to be notified");
            Thread.sleep(3000);

            LOG.info("ProducerMessageHandler.getChannelGroup().unbind()-started");
            ProducerMessageHandler.getChannelGroup().unbind().await();
            LOG.info("ProducerMessageHandler.getChannelGroup().unbind()-finished");

            LOG.info("ProducerMessageHandler.getChannelGroup().close()-started");
            ProducerMessageHandler.getChannelGroup().close().await();
            LOG.info("ProducerMessageHandler.getChannelGroup().close()-finished");

            LOG.info("ProducerMessageHandler.getChannelGroup().clear()-started");
            ProducerMessageHandler.getChannelGroup().clear();
            LOG.info("ProducerMessageHandler.getChannelGroup().unbind()-finished");

            LOG.info("bootstrap.releaseExternalResources()-started");
            bootstrap.releaseExternalResources();
            LOG.info("bootstrap.releaseExternalResources()-finished");

          } catch (InterruptedException e) {
            LOG.error("Interrupted when onClose()", e);
            Thread.currentThread().interrupt();
          }
        }

        //delete idc zk broker producerserver ip
        idcZkClient = KuroroZkUtil.initIDCZk();
        String idcProcuerPath = idcZkProducerServerPath + Constants.SEPARATOR + host;

        if (idcZkClient.exists(idcProcuerPath)) {
          idcZkClient.delete(idcProcuerPath);
        }
        LOG.info("Check Idc Producer node===" + idcProcuerPath);
        LOG.info("ProducerBootStrap-closed");
        pushLocalProducerServerIpListToRemoteIdcZk();
      }
    }, "producer server shutdown finished,consumer server is shutting down ,waiting....");

    // 启动服务
    bootstrap.bind(new InetSocketAddress(Config.getInstance().getProducerPort()));
    LOG.info("[Initialize netty sucessfully, Producer server is ready.]");
    registerServer();
    watchZoneChange();

    this.messageCountUtil.SubmitProducer(new Runnable() {
      @Override
      public void run() {
        try {
          sendProduceMsgToDetector(host);
        } catch (Exception ex) {
          LOG.error("detectorUtil.SubmitProducer thread throw exception: " + ex.getMessage(), ex);
        }
      }
    });
  }

  private void sendProduceMsgToDetector(final String host) {

    Iterator<String> keys = topicMapCount.keySet().iterator();
    List<MessageLog> list = new ArrayList<MessageLog>();
    while (keys.hasNext()) {
      String key = keys.next();
      long count = topicMapCount.get(key).getAndSet(0L);
      long size = 0L;
      if (null != topicMapSize.get(key)) {
        size = topicMapSize.get(key).getAndSet(0L);
      }
      if (count > 0) {
        String[] value = key.split("#");
        MessageLog messageLog = new MessageLog();
        messageLog.setTopicName(value[0]);
        messageLog.setIn(count);
        messageLog.setInSize(size);
        messageLog.setTime(System.currentTimeMillis());
        messageLog.setBroker(host);
        if (value.length > 1) {
          messageLog.setZoneName(value[1]);
        }
        list.add(messageLog);
      }
    }
    analystService.sendCounts(list);

  }

  public AnalystService getAnalystService() {
    return analystService;
  }

  public void setAnalystService(AnalystService analystService) {
    this.analystService = analystService;
  }

  private void pushLocalProducerServerIpListToRemoteIdcZk() {
    if (brokerGroup.equalsIgnoreCase(Constants.KURORO_DEFAULT_BROKER_GROUPNAME)) {
      ZkClient idcZkClient = KuroroZkUtil.initIDCZk();

      List<String> ipList = null;
      if (idcZkClient.exists(idcZkProducerServerPath)) {
        ipList = idcZkClient.getChildren(idcZkProducerServerPath);
      }
      String ipString = "";
      if (ipList == null || ipList.isEmpty()) {
        return;
      }
      for (int i = 0; i < ipList.size(); i++) {
        ipString = ipString + ipList.get(i);
        if (i < (ipList.size() - 1)) {
          ipString = ipString + ",";
        }
      }
      if (ipList != null && !ipList.isEmpty()) {
        Set<String> allIDCName = KuroroZkUtil.allIDCName();
        for (String idcName : allIDCName) {
          if (!idcName.equals(zoneIDCName)) {
            AjaxRequest ajaxRequest = new AjaxRequest();
            ajaxRequest.setS("kuroroService");
            ajaxRequest.setM("updateRemoteProducerServerIpListToLocalIdcZk");
            ajaxRequest.setI(idcName);
            ajaxRequest.setP(zoneIDCName + "@" + ipString);

            try {
              String url = KuroroUtil.currentOpenAPI(idcName);
              Map<String, String> params = new HashMap<String, String>();
              params.put("rmi", JSONObject.toJSONString(ajaxRequest));
              MonitorHttpClientUtil.remoteExcuter(url, params);
            } catch (Exception e) {
              LOG.error("pushLocalProducerServerIpListToRemoteIdcZk", e);
            }
          }
        }
      }
    }
  }

  private void registerServer() {
    lock.lock();

    try {
      //brokerGroup server register on zookeeper
      for (String zoneName : zonesNameSet) {
        zoneZkClient = KuroroZkUtil.initMqZK(zoneName);
        if (zoneZkClient == null) {
          continue;
        }

        //zone design
        String zoneProducerPath = zkRootPath + Constants.SEPARATOR + zoneName + zoneProducerServer;
        String zoneChildProducerPath = zoneProducerPath + Constants.SEPARATOR + host;
        watchServerZkIp(zoneZkClient, zoneProducerPath, zoneChildProducerPath);
      }

      //broker producerserver register idc zk path
      idcZkClient = KuroroZkUtil.initIDCZk();
      String parentPath = idcZkProducerServerPath;
      StringBuilder childPathBuilder = new StringBuilder(parentPath).append(Constants.SEPARATOR)
          .append(host);
      final String childPath = childPathBuilder.toString();
      watchServerZkIp(idcZkClient, parentPath, childPath);

      pushLocalProducerServerIpListToRemoteIdcZk();

    } catch (Exception e) {
      LOG.error(
          (new StringBuilder()).append("Error occour from ZK :").append(e.getMessage()).toString(),
          e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void watchZoneChange() {
  /*
		watch topic zone data change
		try {
			final ZkClient localZk = KuroroZkUtil.initIDCZk();
			String zoneMetaPath = Constants.KURORO_ZK_PARENT_ROOT + Constants.IDCZK_ZONEMETA_PATH;
			if (!localZk.exists(zoneMetaPath)) {
				zoneMetaPath = Constants.IDCZK_ZONEMETA_PATH;
			}
			localZk.subscribeChildChanges(zoneMetaPath, new IZkChildListener() {
				@Override public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
					for (String z : currentChilds) {
						if (!zonesNameSet.contains(z)) {
							String zonePath = parentPath + Constants.SEPARATOR + z;
							if (localZk.exists(zonePath)) {
								Zone zone = localZk.readData(zonePath);
								if (zone.getPlatform().equals(zoneIDCName)) {
									zonesNameSet.add(z);
								}
							}
						}
					}

					registerServer();

					LOG.warn("----producerserver----watchZoneChange-----handleChildChange----####" + currentChilds);
				}
			});
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}*/
  }

  @Override
  protected void watchServerZkIp(final ZkClient localZk, String parentPath,
      final String childPath) {

    if (!localZk.exists(parentPath)) {
      localZk.createPersistent(parentPath, true);
    }

    if (!localZk.exists(childPath)) {
      localZk.createEphemeral(childPath, hi);
      LOG.info("producerserver node created on {}", childPath);
    }

    localZk.subscribeStateChanges(new IZkStateListener() {
      @Override
      public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
        LOG.info("KeeperState:" + keeperState);
      }

      @Override
      public void handleNewSession() throws Exception {
        LOG.info("New session");
      }
    });
    localZk.subscribeChildChanges(parentPath, new IZkChildListener() {

      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds)
          throws Exception {
        if (parentPath != null) {
          if (!currentChilds.contains(host)) {
            if (!localZk.exists(childPath)) {
              localZk.createEphemeral(childPath, hi);
            }
          }
        }
      }

    });
  }
}

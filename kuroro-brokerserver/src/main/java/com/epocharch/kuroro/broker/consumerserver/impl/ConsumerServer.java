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

package com.epocharch.kuroro.broker.consumerserver.impl;

import com.epocharch.common.util.SystemUtil;
import com.epocharch.kuroro.broker.BrokerServer;
import com.epocharch.kuroro.broker.Config;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.monitor.CloseMonitor;
import com.epocharch.kuroro.common.inner.monitor.CloseMonitor.CloseHook;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.wrap.WrappedConsumerMessage;
import com.epocharch.kuroro.common.inner.wrap.WrappedMessage;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.protocol.json.JsonDecoder;
import com.epocharch.kuroro.common.protocol.json.JsonEncoder;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkStateListener;
import com.epocharch.zkclient.ZkClient;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.zookeeper.Watcher;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerServer extends BrokerServer {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerServer.class);
  private final String brokerGroup = System
      .getProperty("brokerGroup", Constants.KURORO_DEFAULT_BROKER_GROUPNAME);
  private ConsumerWorkerManager consumerWorkerManager;
  private String zoneConsumerServer = null;
  private String zoneIDCName = null;
  private String zoneZkRoot = null;
  private String idcZkConsumerServerPath = null;

  public ConsumerServer() {
  }

  public void start() {

    zonesNameSet = KuroroZkUtil.localIdcAllZones();
    idcZkClient = KuroroZkUtil.initIDCZk();
    zoneIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
    zoneZkRoot = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH);
    zoneConsumerServer =
        Constants.ZONE_BROKER + Constants.SEPARATOR + brokerGroup + Constants.ZONE_BROKER_CONSUMER;
    idcZkConsumerServerPath =
        System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH) + Constants.ZONE_BROKER
            + Constants.SEPARATOR + brokerGroup
            + Constants.ZONE_BROKER_CONSUMER;

    //host = IPUtil.getFirstNoLoopbackIP4Address();
    host = SystemUtil.getLocalhostIp();
    hi = new HostInfo(host, Constants.KURORO_CONSUMER_PORT, 1);

    consumerWorkerManager.init();
    LOG.info("start working....");
    final ServerBootstrap bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(new DefaultThreadFactory("ConsumerServer-BossExecutor")),
            Executors
                .newCachedThreadPool(new DefaultThreadFactory("ConsumerServer-WorkerExecutor"))));
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ConsumerMessageHandler handler = new ConsumerMessageHandler(consumerWorkerManager);
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
        pipeline.addLast("jsonDecoder", new JsonDecoder(WrappedConsumerMessage.class));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("jsonEncoder", new JsonEncoder(WrappedMessage.class));
        pipeline.addLast("handler", handler);
        return pipeline;
      }
    });
    // 启用close monitor
    CloseMonitor closeMonitor = new CloseMonitor();
    int port = Integer.parseInt(
        System.getProperty("consumerCloseMonitorPort", Constants.CONSUMER_CLOSE_MONITOR_PORT));
    closeMonitor.start(port, true, new CloseHook() {
      @Override
      public void onClose() {

        try {

          LOG.info("consumerWorkerManager.close()-started");
          consumerWorkerManager.close();
          LOG.info("consumerWorkerManager.close()-finished");

          LOG.info("MessageServerHandler.getChannelGroup().unbind()-started");
          ConsumerMessageHandler.getChannelGroup().unbind().await();
          LOG.info("MessageServerHandler.getChannelGroup().unbind()-finished");

          LOG.info("MessageServerHandler.getChannelGroup().close()-started");
          ConsumerMessageHandler.getChannelGroup().close().await();
          LOG.info("MessageServerHandler.getChannelGroup().close()-finished");

          LOG.info("MessageServerHandler.getChannelGroup().clear()-started");
          ConsumerMessageHandler.getChannelGroup().clear();
          LOG.info("MessageServerHandler.getChannelGroup().unbind()-finished");

          LOG.info("bootstrap.releaseExternalResources()-started");
          bootstrap.releaseExternalResources();
          LOG.info("bootstrap.releaseExternalResources()-finished");

          LOG.info("zkclient.close()-started");

          for (String zoneName : zonesNameSet) {
            zoneZkClient = KuroroZkUtil.initMqZK(zoneName);
            if (zoneZkClient == null) {
              continue;
            }
            //zone path
            String zoneConsumerServerPath =
                zoneZkRoot + Constants.SEPARATOR + zoneName + zoneConsumerServer
                    + Constants.SEPARATOR + host;
            if (zoneZkClient.exists(zoneConsumerServerPath)) {
              zoneZkClient.delete(zoneConsumerServerPath);
            }
            LOG.info("zkclient.close()-finished" + zoneConsumerServerPath);
          }

          //delete idc zk broker consumerserver ip
          idcZkClient = KuroroZkUtil.initIDCZk();
          String idcConsumerPath = idcZkConsumerServerPath + Constants.SEPARATOR + host;
          if (idcZkClient.exists(idcConsumerPath)) {
            idcZkClient.delete(idcConsumerPath);
          }
          LOG.info("Check Idc Producer node===" + idcConsumerPath);
        } catch (InterruptedException e) {
          LOG.error("Interrupted when onClose()", e);
          Thread.currentThread().interrupt();
        } finally {
          LOG.info("ConsumerBootStrap-closed");
        }

      }
    }, "consumer server shutdown finished!!!");
    // 启动服务
    bootstrap.bind(new InetSocketAddress(Config.getInstance().getConsumerPort()));
    LOG.info("[Initialize netty sucessfully, Consumer server is ready.]");
    registerServer();
    watchZoneChange();
  }

  public void setConsumerWorkerManager(ConsumerWorkerManager consumerWorkerManager) {
    this.consumerWorkerManager = consumerWorkerManager;
  }

  private void registerServer() {
    lock.lock();
    try {
      // zk注册(default path)
      for (String zoneName : zonesNameSet) {
        zoneZkClient = KuroroZkUtil.initMqZK(zoneName);
        if (zoneZkClient == null) {
          continue;
        }
        //zone path
        String zonePath = zoneZkRoot + Constants.SEPARATOR + zoneName + zoneConsumerServer;
        StringBuilder zoneChildPathBuilder = new StringBuilder(zonePath).append(Constants.SEPARATOR)
            .append(host);
        final String zoneChildPath = zoneChildPathBuilder.toString();
        watchServerZkIp(zoneZkClient, zonePath, zoneChildPath);
      }

      //broker consumerserver register idc zk path
      idcZkClient = KuroroZkUtil.initIDCZk();
      String parentPath = idcZkConsumerServerPath;
      StringBuilder childPathBuilder = new StringBuilder(parentPath).append(Constants.SEPARATOR)
          .append(host);
      final String childPath = childPathBuilder.toString();
      watchServerZkIp(idcZkClient, parentPath, childPath);

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
 watch zone meta data change
 try {
      final ZkClient localZk = KuroroZkUtil.initIDCZk();
      String zoneMetaPath = Constants.KURORO_ZK_PARENT_ROOT + Constants.IDCZK_ZONEMETA_PATH;
      if (!localZk.exists(zoneMetaPath)) {
        zoneMetaPath = Constants.IDCZK_ZONEMETA_PATH;
      }
      localZk.subscribeChildChanges(zoneMetaPath, new IZkChildListener() {
        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds)
            throws Exception {
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
          LOG.warn("----consumerServer----watchZoneChange-----handleChildChange----####"
              + currentChilds);
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
      LOG.info("consumerserver node created on {}", childPath);
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

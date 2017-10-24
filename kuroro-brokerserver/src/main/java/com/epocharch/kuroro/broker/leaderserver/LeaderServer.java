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

package com.epocharch.kuroro.broker.leaderserver;

import com.epocharch.common.util.SystemUtil;
import com.epocharch.kuroro.broker.BrokerServer;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.monitor.CloseMonitor;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkStateListener;
import com.epocharch.zkclient.ZkClient;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.zookeeper.Watcher;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderServer extends BrokerServer {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderServer.class);
  private final int leaderIoWorkerCount = Integer
		  .parseInt(System
          .getProperty("leader.io.workers", "" + Runtime.getRuntime().availableProcessors() * 4));
  private final String brokerGroup = System
      .getProperty("brokerGroup", Constants.KURORO_DEFAULT_BROKER_GROUPNAME);
  private LeaderWorkerManager leaderWorkerManager;
  //zone design
  private String zkRootPath = null;
  private String zoneIDCName = null;
  private String zoneLeaderServerPath = null;
  private String idcZkLeaderServerPath = null;

  public void start() {
    KuroroZkUtil.initBrokerIDCMetas();
    KuroroZkUtil.initClientIDCMetas();
    zonesNameSet = KuroroZkUtil.localIdcAllZones();

    zoneIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
    zkRootPath = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH);
    zoneLeaderServerPath =
        Constants.ZONE_BROKER + Constants.SEPARATOR + brokerGroup + Constants.ZONE_BROKER_LEADER;
    idcZkLeaderServerPath =
        System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH) + Constants.ZONE_BROKER
            + Constants.SEPARATOR + brokerGroup
            + Constants.ZONE_BROKER_LEADER;
    String localZone = System.getProperty(InternalPropKey.ZONE_LOCALZONE);
    host = SystemUtil.getLocalhostIp();
    hi = new HostInfo(host, Constants.KURORO_LEADER_PORT, 1);

    final ServerBootstrap bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(new DefaultThreadFactory("LeaderServer-BossExecutor")),
            Executors.newCachedThreadPool(new DefaultThreadFactory("LeaderServer-WorkerExecutor")),
            leaderIoWorkerCount));
    bootstrap.setPipelineFactory(new LeaderServerPipelineFactory(leaderWorkerManager));

    // 启用close monitor
    CloseMonitor closeMonitor = new CloseMonitor();
    int port = Integer.parseInt(
        System.getProperty("leaderCloseMonitorPort", Constants.LEADER_CLOSE_MONITOR_PORT));
    closeMonitor.start(port, false, new CloseMonitor.CloseHook() {
      @Override
      public void onClose() {
        try {

          LOG.info("LeaderWorkerManager.close()-started");
          leaderWorkerManager.close();
          LOG.info("LeaderWorkerManager.close()-finished");

          LOG.info("LeaderMessageHandler.getChannelGroup().unbind()-started");
          LeaderMessageHandler.getChannelGroup().unbind().await();
          LOG.info("LeaderMessageHandler.getChannelGroup().unbind()-finished");

          LOG.info("LeaderMessageHandler.getChannelGroup().close()-started");
          LeaderMessageHandler.getChannelGroup().close().await();
          LOG.info("LeaderMessageHandler.getChannelGroup().close()-finished");

          LOG.info("LeaderMessageHandler.getChannelGroup().clear()-started");
          LeaderMessageHandler.getChannelGroup().clear();
          LOG.info("LeaderMessageHandler.getChannelGroup().unbind()-finished");

          LOG.info("bootstrap.releaseExternalResources()-started");
          bootstrap.releaseExternalResources();
          LOG.info("bootstrap.releaseExternalResources()-finished");

          LOG.info("zkclient.close()-started");

          for (String zoneName : zonesNameSet) {
            zoneZkClient = KuroroZkUtil.initMqZK(zoneName);
            if (zoneZkClient == null) {
              continue;
            }
            String _zoneLeaderServerPath =
                zkRootPath + Constants.SEPARATOR + zoneName + zoneLeaderServerPath
                    + Constants.SEPARATOR + host;
            if (zoneZkClient.exists(_zoneLeaderServerPath)) {
              zoneZkClient.delete(_zoneLeaderServerPath);
            }
            LOG.info("zkclient.close()--finshed===" + _zoneLeaderServerPath);
          }

          zoneZkClient.unsubscribeAll();

          //idc zk leaderserver path
          idcZkClient = KuroroZkUtil.initIDCZk();
          String _idcZkLeaderPath = idcZkLeaderServerPath + Constants.SEPARATOR + host;
          if (idcZkClient.exists(_idcZkLeaderPath)) {
            idcZkClient.delete(_idcZkLeaderPath);
          }
          idcZkClient.unsubscribeAll();
          LOG.info("zk.client.close()---finished====" + _idcZkLeaderPath);
        } catch (InterruptedException e) {
          LOG.error("Interrupted when onClose()", e);
          Thread.currentThread().interrupt();
        }
        LOG.info("LeaderBootStrap-closed");

      }
    }, "leader server shutdown finished");

    // 启动服务
    bootstrap.bind(new InetSocketAddress(Constants.KURORO_LEADER_PORT));
    LOG.info("[Initialize netty sucessfully, Lead server is ready.]");

    registerServer();

    watchZoneChange();
  }

  public void registerServer() {
    lock.lock();

    try {
      for (String zoneName : zonesNameSet) {

        LOG.warn("=====leader connected Zone======" + zoneName);
        //zonename is changing
        String _zoneLeaderServerPath =
            zkRootPath + Constants.SEPARATOR + zoneName + zoneLeaderServerPath;
        zoneZkClient = KuroroZkUtil.initMqZK(zoneName);
        if (zoneZkClient == null) {
          continue;
        }
        // zookeeper register(zone design path)
        String zonePath = _zoneLeaderServerPath;
        StringBuilder zoneChildPathBuilder = new StringBuilder(zonePath).append(Constants.SEPARATOR)
            .append(host);
        final String zoneChildPath = zoneChildPathBuilder.toString();
        watchServerZkIp(zoneZkClient, zonePath, zoneChildPath);
      }

      //broker leaderserver register idc zk path
      idcZkClient = KuroroZkUtil.initIDCZk();
      String parentPath = idcZkLeaderServerPath;
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

  public void setLeaderWorkerManager(LeaderWorkerManager leaderWorkerManager) {
    this.leaderWorkerManager = leaderWorkerManager;
  }

  @Override
  protected void watchZoneChange() {
    //watch zones meta data change
		/*	try {
			final ZkClient localZk = KuroroZkUtil.initIDCZk();
			String zoneMetaPath = Constants.ZONE_ZK_PARENTROOT + Constants.IDCZK_ZONEMETA_PATH;
			if (!localZk.exists(zoneMetaPath)) {
				zoneMetaPath = Constants.IDCZK_ZONEMETA_PATH;
			}
			localZk.subscribeChildChanges(zoneMetaPath, new IZkChildListener() {
				@Override
				public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
					for (String z : currentChilds) {
						if (!zonesNameSet.contains(z)) {
							String zonePath = parentPath +Constants.SEPARATOR + z;
							if (localZk.exists(zonePath)) {
								Zone zone = localZk.readData(zonePath);
								if (zone.getPlatform().equals(zoneIDCName)){
									zonesNameSet.add(z);
								}
							}
						}
					}
					registerServer();
					LOG.warn("----leaderserver----watchZoneChange-----handleChildChange----####" + currentChilds);
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
      LOG.info("leaderserver node created on {}", childPath);
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

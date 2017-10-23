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

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.jmx.support.JmxSpringUtil;
import com.epocharch.kuroro.common.netty.component.DefaultThreadFactory;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.netty.component.SimpleThreadPool;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkStateListener;
import com.epocharch.zkclient.ZkClient;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.Watcher;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

public class LeaderClientManager {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderClientManager.class);
  public static AtomicLong sequence = new AtomicLong(0L);
  private final String brokerGroup = System
		  .getProperty("brokerGroup", Constants.KURORO_DEFAULT_BROKER_GROUPNAME);
  private final ZkClient idcZkClient = KuroroZkUtil.initIDCZk();
  private Executor bossExecutor;
  private Executor workerExecutor;
  private SimpleThreadPool clientResponseThreadPool;
  private Map<String, LeaderClient> leaderClientCache = new ConcurrentHashMap<String, LeaderClient>();
  private List<String> hosts = null;
  private volatile List<String> sortHosts = null;
  private List<String> mongos = null;
  private volatile List<String> sortMongos = new ArrayList<String>();
  private JmxSpringUtil jmxSpringUtil;
  private NioClientSocketChannelFactory nioClientSocketChannelFactory;
  private ScheduledExecutorService cycExecutorService = Executors
      .newScheduledThreadPool(Runtime.getRuntime().availableProcessors(),
          new DefaultThreadFactory("Leader-Cyc"));
  //zone design
  private String idcZkRootPath = null;
  private String zoneIDCName = null;
  private String idcZkLeaderServerPath = null;
  private String idcZkTopicPath = null;
  private IZkChildListener hostsListener = new IZkChildListener() {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      LOG.info("Hosts changed {}->{}", hosts, currentChilds);
      if (!currentChilds.equals(hosts)) {
        hosts = currentChilds;
        sortHosts();
      }
      LOG.info("Hosts {}", hosts);
    }

  };
  private IZkChildListener topicListener = new IZkChildListener() {

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      LOG.info("Topics changed {}->{}", mongos, currentChilds);
      if (!currentChilds.equals(mongos)) {
        mongos = currentChilds;
        sortTopic();
      }
      LOG.info("Mongos {}", mongos);
    }

  };

  public LeaderClientManager() {
    init();
  }

  public LeaderClientManager(JmxSpringUtil jmxSpringUtil) {
    init();
    this.jmxSpringUtil = jmxSpringUtil;
    String mBeanName = "LeaderClientManager-Topic-Info";
    jmxSpringUtil.registerMBean(mBeanName, new MonitorBean(this));
  }

  private void init() {

    zoneIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
    idcZkRootPath = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH);
    idcZkLeaderServerPath =
        idcZkRootPath + Constants.ZONE_BROKER + Constants.SEPARATOR + brokerGroup
            + Constants.ZONE_BROKER_LEADER;
    idcZkTopicPath = idcZkRootPath + Constants.ZONE_TOPIC;

    this.bossExecutor = Executors
        .newCachedThreadPool(new DefaultThreadFactory("Leader-Client-BossExecutor"));
    this.workerExecutor = Executors
        .newCachedThreadPool(new DefaultThreadFactory("Leader-Client-WorkerExecutor"));

    this.clientResponseThreadPool = new SimpleThreadPool("Client-ResponseProcessor", 50, 100,
        new ArrayBlockingQueue<Runnable>(
            Integer.parseInt(System.getProperty("leader.client.response.queue.size", "1024"))),
        new ThreadPoolExecutor.DiscardPolicy());
    this.nioClientSocketChannelFactory = new NioClientSocketChannelFactory(this.bossExecutor,
        this.workerExecutor);
    ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);

    try {
      hosts = idcZkClient.getChildren(idcZkLeaderServerPath);
      for (int i = 0; i < 10; i++) {
        if (hosts == null || hosts.isEmpty()) {
          Thread.sleep(2000L);
          hosts = idcZkClient.getChildren(idcZkLeaderServerPath);
        } else {
          break;
        }
      }

      sortHosts();

      mongos = idcZkClient.getChildren(idcZkRootPath + Constants.ZONE_TOPIC);
      if (mongos == null || mongos.size() == 0) {
        LOG.warn("can not find Kuroro topic from idc zookeeper !!!");
      }

      LOG.info("Mongos {}", mongos);
      sortTopic();
      LOG.info("Sort mongos {}", sortMongos);
      subscribeLeader();
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e.getCause());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e.getCause());
    }
  }

  private void sortHosts() {
    if (hosts != null && !hosts.isEmpty()) {
      List<String> hostList = new ArrayList<String>(hosts);
      Collections.sort(hostList, new IpComparator());
      sortHosts = hostList;
    } else {
      sortHosts = null;
    }
  }

  private void sortTopic() {
    String child = idcZkTopicPath;
    if (mongos != null && !mongos.isEmpty()) {
      List<TopicConfigDataMeta> topicDataList = new ArrayList<TopicConfigDataMeta>();
      for (String topic : mongos) {
        String childPath = child + Constants.SEPARATOR + topic;
        try {
          if (idcZkClient.exists(childPath)) {
            TopicConfigDataMeta data = idcZkClient.readData(childPath, true);
            if (data != null) {
              if (data.getTimeStamp() == null) {
                data.setTimeStamp(Long.MAX_VALUE / 2);
              }
              topicDataList.add(data);
            }
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e.getCause());
        }

      }
      if (topicDataList.size() > 0) {
        ComparatorTopic topicComparator = new ComparatorTopic();
        Collections.sort(topicDataList, topicComparator);
        List<String> sortMongoList = new ArrayList<String>();
        for (int i = 0; i < topicDataList.size(); i++) {
          sortMongoList.add(topicDataList.get(i).getTopicName());
        }
        sortMongos = sortMongoList;
      }
    } else {
      sortMongos = null;
    }
  }

  private void subscribeLeader() {

    idcZkClient.subscribeChildChanges(idcZkLeaderServerPath, hostsListener);
    idcZkClient.subscribeChildChanges(idcZkTopicPath, topicListener);
    idcZkClient.subscribeStateChanges(new IZkStateListener() {
      @Override
      public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
        switch (keeperState) {
          case SyncConnected:
            idcZkClient.subscribeChildChanges(idcZkLeaderServerPath, hostsListener);
            idcZkClient.subscribeChildChanges(idcZkTopicPath, topicListener);
            LOG.info("keeperState:" + keeperState);
            hosts = idcZkClient.getChildren(idcZkLeaderServerPath);
            sortHosts();
            LOG.info("Hosts after keeperState changed:{}", hosts);
            mongos = idcZkClient.getChildren(idcZkTopicPath);
            sortTopic();
            LOG.info("Topics after keeperState changed:{}", mongos);
            break;
        }
      }

      @Override
      public void handleNewSession() throws Exception {
        idcZkClient.subscribeChildChanges(idcZkLeaderServerPath, hostsListener);
        idcZkClient.subscribeChildChanges(idcZkTopicPath, topicListener);
        LOG.info("New zk session");
        hosts = idcZkClient.getChildren(idcZkLeaderServerPath);
        sortHosts();
        LOG.info("Hosts after new session created:{}", hosts);
        mongos = idcZkClient.getChildren(idcZkTopicPath);
        sortTopic();
        LOG.info("Topics after keeperState changed:{}", mongos);
      }
    });

  }

  public LeaderClient getLeaderClient(WrapTopicConsumerIndex topicConsumer) {
    try {
      HostInfo n = select(topicConsumer);
      if (n != null) {
        if (leaderClientCache.containsKey(topicConsumer.getTopicName())) {
          LeaderClient leaderClient = leaderClientCache.get(topicConsumer.getTopicName());
          if (leaderClient != null) {
            if (leaderClient.getAddress().equals(n.getConnect())) {
              if (!leaderClient.isConnected()) {
                leaderClient.connect();
              }
              return leaderClient;
            } else {
              if (leaderClient.isConnected()) {
                MessageIDPair d = leaderClient.sleepLeaderWork(topicConsumer.getTopicName());
                if (d != null && d.getCommand().equals("stopped")) {
                  leaderClient.close();
                }
              }
            }
          }
        }

        LeaderClient leaderClient = new LeaderClient(n.getHost(), n.getPort(), this,
            topicConsumer.getTopicName());
        leaderClient.connect();
        if (leaderClient.isConnected()) {
          leaderClientCache.put(topicConsumer.getTopicName(), leaderClient);
          return leaderClient;
        }
      }
    } catch (Exception e) {
      LOG.error("get leader-" + e.getMessage(), e.getCause());
    }
    return null;
  }

  public Executor getBossExecutor() {
    return bossExecutor;
  }

  public Executor getWorkerExecutor() {
    return workerExecutor;
  }

  public JmxSpringUtil getJmxSpringUtil() {
    return jmxSpringUtil;
  }

  public NioClientSocketChannelFactory getNioClientSocketChannelFactory() {
    return nioClientSocketChannelFactory;
  }

  public SimpleThreadPool getClientResponseThreadPool() {
    return clientResponseThreadPool;
  }

  public void setClientResponseThreadPool(SimpleThreadPool clientResponseThreadPool) {
    this.clientResponseThreadPool = clientResponseThreadPool;
  }

  public HostInfo select(WrapTopicConsumerIndex topicConsumer) {
    String topic = topicConsumer.getTopicName();

    List<String> childPaths = sortMongos;
    List<String> leaderServerHosts = sortHosts;
	  if (childPaths == null) {
		  return null;
	  }
	  if (childPaths.size() <= 0) {
		  return null;
	  }
	  if (leaderServerHosts == null || leaderServerHosts.size() == 0) {
		  return null;
	  }
    int count = leaderServerHosts.size();
    int index = childPaths.indexOf(topic);
    if (index != -1) {
      String host = null;
		if (count == 1) {
			host = leaderServerHosts.get(0);
		} else {
			try {
				int selectNum = index % count;
				host = leaderServerHosts.get(selectNum);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				host = leaderServerHosts.get(0);
			}
		}
		if (host == null) {
			host = leaderServerHosts.get(0);
		}
      HostInfo hi = new HostInfo(host, Constants.KURORO_LEADER_PORT, 1);
      return hi;
    }
    return null;
  }

  public ScheduledExecutorService getCycExecutorService() {
    return cycExecutorService;
  }

  @ManagedResource(description = "leaderclient select leader server info")
  public static class MonitorBean {

    private final WeakReference<LeaderClientManager> leaderClientManager;

    private MonitorBean(LeaderClientManager clientManager) {
      this.leaderClientManager = new WeakReference<LeaderClientManager>(clientManager);
    }

    @ManagedAttribute
    public String getTopicConnections() {
      if (this.leaderClientManager.get() != null) {
        StringBuilder sb = new StringBuilder();
        if (this.leaderClientManager.get().leaderClientCache != null) {
          for (String topic : this.leaderClientManager.get().leaderClientCache.keySet()) {
            sb.append(topic).append("(isConnected:")
                .append(this.leaderClientManager.get().leaderClientCache.get(topic).getAddress())
                .append(')');
          }
        }
        return sb.toString();
      }
      return null;
    }

    @ManagedAttribute
    public String getLeading() {
      ArrayList<String> leading = new ArrayList<String>();
      if (this.leaderClientManager.get() != null) {
        Iterator<String> it = this.leaderClientManager.get().leaderClientCache.keySet().iterator();
        while (it.hasNext()) {
          leading.add(it.next());
        }
      }
      return leading.toString();
    }

    @ManagedAttribute
    public String getBrokers() {
      if (this.leaderClientManager.get() != null) {
        return this.leaderClientManager.get().hosts.toString();
      }
      return null;
    }

    @ManagedAttribute
    public String getSortBrokers() {
      if (this.leaderClientManager.get() != null) {
        return this.leaderClientManager.get().sortHosts.toString();
      }
      return null;
    }

    @ManagedAttribute
    public String getTopics() {
      if (this.leaderClientManager.get() != null) {
        return this.leaderClientManager.get().mongos.toString();
      }
      return null;
    }

    @ManagedAttribute
    public String getSortTopics() {
      if (this.leaderClientManager.get() != null) {
        return this.leaderClientManager.get().sortMongos.toString();
      }
      return null;
    }
  }

  private class ComparatorTopic implements Comparator<TopicConfigDataMeta> {

    @Override
    public int compare(TopicConfigDataMeta o1, TopicConfigDataMeta o2) {
      int flag = o1.getTimeStamp().compareTo(o2.getTimeStamp());
      if (flag == 0) {
        flag = o1.getTopicName().compareTo(o2.getTopicName());
      }
      return flag;
    }

  }

  class IpComparator implements Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
      String[] parts1 = o1.split("\\.");
      String[] parts2 = o2.split("\\.");

      for (int i = 0; i < 4; i++) {
        if (parts1[i].equals(parts2[i])) {
          continue;
        } else {
          return Integer.parseInt(parts1[i]) - Integer.parseInt(parts2[i]);
        }
      }
      return 0;
    }
  }
}
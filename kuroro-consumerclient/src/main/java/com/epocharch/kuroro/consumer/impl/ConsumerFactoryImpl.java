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

package com.epocharch.kuroro.consumer.impl;

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.consumer.ConsumerOffset;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.TimeUtil;
import com.epocharch.kuroro.common.message.Destination;
import com.epocharch.kuroro.common.message.TransferDestination;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.consumer.BrokerGroupRouteManager;
import com.epocharch.kuroro.consumer.Consumer;
import com.epocharch.kuroro.consumer.ConsumerConfig;
import com.epocharch.kuroro.consumer.ConsumerFactory;
import com.epocharch.kuroro.consumer.impl.inner.ConsumerImpl;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.ZkClient;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsumerFactoryImpl implements ConsumerFactory {

  private static final Logger LOG = LoggerFactory
      .getLogger(ConsumerFactoryImpl.class);
  private static final Map<String, LinkedBlockingQueue<Consumer>> connectionPool = new ConcurrentHashMap<String, LinkedBlockingQueue<Consumer>>();
  public static Map<String, Consumer> consumerMap = new ConcurrentHashMap<String, Consumer>();
  private static ConsumerFactoryImpl instance = new ConsumerFactoryImpl();
  private static Map<String, BrokerGroupRouteManager> groupMap = new ConcurrentHashMap<String, BrokerGroupRouteManager>();
  private BrokerGroupRouteManager routerManager;
  private String zoneIDCName;
  private String localZoneName;
  private String zoneRootPath;
  private String idcZkRoot;
  private ZkClient zkClient = null;
  private boolean isIdcProduct;
  private String poolId = null;

  private ConsumerFactoryImpl() {
    init();
  }

  public static ConsumerFactory getInstance() {
    return instance;
  }

  public static Map<String, Consumer> getConsumerMap() {
    return consumerMap;
  }

  public static void setConsumerMap(Map<String, Consumer> consumerMap) {
    ConsumerFactoryImpl.consumerMap = consumerMap;
  }

  public static Map<String, BrokerGroupRouteManager> getGroupMap() {
    return groupMap;
  }

  public static void setGroupMap(Map<String, BrokerGroupRouteManager> groupMap) {
    ConsumerFactoryImpl.groupMap = groupMap;
  }

  public void init() {

  }

  /**
   * @param dest 消息目的地，类型为{@link Destination}
   * @param consumerId 消费者id
   * @param config consumer配置信息
   */
  @Override
  public Consumer createConsumer(final Destination dest, final String consumerId,
      final ConsumerConfig config) {

    if (dest == null || consumerId == null || config == null) {
      throw new NullPointerException("Null param in createConsumer");
    }

    //监听topic变化，随时准备接收异地消息
    String transTopicPath = null;
    isIdcProduct = KuroroUtil.isIdcProduction();
    System.setProperty(InternalPropKey.POOL_LEVEL, String.valueOf(isIdcProduct));
    zkClient = KuroroZkUtil.initLocalMqZk();

    //offset check
    if (config.getConsumerOffset() != null && !KuroroUtil
        .isBlankString(config.getConsumerOffset().getDateTime())) {
      String offsetTime = config.getConsumerOffset().getDateTime();
      int hourUnit = TimeUtil.dateTimeCompare(offsetTime, null);
      if (hourUnit > 3 * 24) {
        config.setConsumerOffset(null);
        LOG.warn("consumer offset time can not more than 3 day, please check offset time!");
      } else {
        Long offset = TimeUtil.dateToTimestamp(offsetTime);
        config.getConsumerOffset().setOffsetValue(offset);
        config.getConsumerOffset().setType(ConsumerOffset.OffsetType.CUSTOMIZE_OFFSET);
      }
    }

    if (isIdcProduct) {
      KuroroZkUtil.initBrokerIDCMetas();
      idcZkRoot = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH);
      transTopicPath = idcZkRoot + Constants.ZONE_TOPIC;
      localZoneName = System.getProperty(InternalPropKey.ZONE_LOCALZONE);
      config.setIdcProduct(true);
    } else {
      KuroroZkUtil.initClientIDCMetas();
      zoneIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
      localZoneName = System.getProperty(InternalPropKey.ZONE_LOCALZONE);
      zoneRootPath = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH) +
          Constants.SEPARATOR + localZoneName;
      transTopicPath = zoneRootPath + Constants.ZONE_TOPIC;
    }
    poolId = KuroroUtil.poolIdName();

    final Consumer local = createLocalConsumer(dest, consumerId, config);
    List<Consumer> transferredConsumers = createTransferredConsumer(dest, consumerId, config);
    LOG.info("Need {} transferred consumer(s) in {}",
        transferredConsumers == null ? 0 : transferredConsumers.size(), consumerId);
    if (transferredConsumers != null && !transferredConsumers.isEmpty()
        && local instanceof ConsumerImpl) {
      ConsumerImpl localConsumer = (ConsumerImpl) local;
      for (Consumer transferredConsumer : transferredConsumers) {
        if (transferredConsumer instanceof ConsumerImpl) {
          ConsumerImpl impl = (ConsumerImpl) transferredConsumer;
          localConsumer.addChild(impl);
          LOG.info("Add a transferred consumer: {}", impl.toString());
        } else {
          LOG.warn("Ignore a non-ConsumerImpl consumer: {}", transferredConsumer.toString());
        }
      }
    }

    zkClient.subscribeChildChanges(transTopicPath, new IZkChildListener() {
      @Override
      public void handleChildChange(String s, List<String> strings) throws Exception {
        synchronized (ConsumerFactoryImpl.this) {
          Set<String> zones = KuroroZkUtil.allIdcZoneName();

          if (local instanceof ConsumerImpl) {
            ConsumerImpl c = (ConsumerImpl) local;
            for (String topic : strings) {
              for (String zoneName : zones) {
                TransferDestination transferDestination = TransferDestination
                    .topic(dest.getName(), zoneName, consumerId);
                if (transferDestination.getName().equals(topic)) {
                  //是否在child中
                  if (!((ConsumerImpl) local).isConsumerExist(transferDestination)) {
                    ConsumerImpl ci = (ConsumerImpl) initConsumer(transferDestination, consumerId,
                        config);
                    c.addChild(ci);
                    LOG.info("Add a new coming transferred consumer: {}", ci);
                  } else {
                    LOG.info("Ignore an exist transferred consumer: {}", transferDestination);
                  }
                }
              }
            }
          } else {
            LOG.error("Impossible: Not ConsumerImpl");//Impossible
          }
        }
      }
    });
    return local;
  }

  /**
   * 创建本地消费者，只消费本地Zone，忽略来自其它Zone的消息
   */
  @Override
  public Consumer createLocalConsumer(Destination dest, String consumerId, ConsumerConfig config) {
    Consumer consumer = initConsumer(dest, consumerId, config);
    return consumer;
  }

  /**
   * 创建异地zone的消费者
   */
  @Override
  public List<Consumer> createTransferredConsumer(Destination dest, String consumerId,
      ConsumerConfig config) {
    return createTransferConsumer(dest, consumerId, config);
  }

  private List<Consumer> createTransferConsumer(Destination dest, String consumerId,
      ConsumerConfig config) {
    Set<String> zoneNames = KuroroZkUtil.allIdcZoneName();
    Set<String> idcNames = KuroroZkUtil.allIDCName();
    if (zoneNames == null || zoneNames.isEmpty() || idcNames == null || idcNames.isEmpty()) {
      return null;
    }

    List<Consumer> consumers = new ArrayList<Consumer>();
    List<String> topics = null;
    String transRootPath = null;
    if (isIdcProduct) {
      topics = zkClient.getChildren(idcZkRoot + Constants.ZONE_TOPIC);
      transRootPath = idcZkRoot + Constants.ZONE_TOPIC;
    } else {
      topics = zkClient.getChildren(zoneRootPath + Constants.ZONE_TOPIC);
      transRootPath = zoneRootPath + Constants.ZONE_TOPIC;
    }
    for (String topic : topics) {
      //new transfer on idcs
      for (String idcName : idcNames) {
        if (KuroroUtil.isBlankString(idcName) || KuroroZkUtil.localIDCName().equals(idcName)) {
          continue;
        }
        TransferDestination transferDestination = TransferDestination
            .topic(dest.getName(), idcName, consumerId);
        if (transferDestination.getName().equals(topic)) {
          String tranPath = transRootPath + Constants.SEPARATOR + transferDestination.getName();
          if (zkClient.exists(tranPath)) {
            consumers.add(initConsumer(transferDestination, consumerId, config));
          }
        }
      }
    }
    return consumers;
  }

  @Override
  public Consumer createConsumer(Destination dest, String consumerId) {
    return createConsumer(dest, consumerId, new ConsumerConfig());
  }

  @Override
  public Consumer createConsumer(Destination dest) {
    String consumerId = dest.getName() + "autoconsumer";
    return createConsumer(dest, consumerId);
  }

  private Consumer initConsumer(Destination dest, String consumerId,
      ConsumerConfig config) {
    if (isIdcProduct) {
      return initIdcConsumer(dest, consumerId, config);
    } else {
      return initZoneConsumer(dest, consumerId, config);
    }
  }

  //init zone consumer
  private Consumer initZoneConsumer(Destination dest, String consumerId,
      ConsumerConfig config) {

    String brokerGroupPath = null;
    String brokerGroup = null;
    String topicName = dest.getName();
    TopicConfigDataMeta zoneTb = null;
    String groupContext = null;

    if (config.isConsumeLocalZone()) {
      Set<String> consumeZoneFilter = new HashSet<String>();
      if (!KuroroUtil.isBlankString(localZoneName)) {
        consumeZoneFilter.add(localZoneName);
        config.setConsumeLocalZoneFilter(MessageFilter.createInSetMessageFilter(consumeZoneFilter));
      } else {
        LOG.error("localZoneName is not null! please check ");
      }
    }
    try {
      String zoneTopicPath = zoneRootPath + Constants.ZONE_TOPIC + Constants.SEPARATOR + topicName;
      if (zkClient.exists(zoneTopicPath)) {
        zoneTb = (TopicConfigDataMeta) zkClient.readData(zoneTopicPath);
        if (null == zoneTb) {
            throw new Exception("Can not find topic[" + topicName
                + "] register info, please check or contact MQ administrator.");
        }
      }

      groupContext = zoneTb.getBrokerGroup();
      if (KuroroUtil.isBlankString(groupContext)) {
        zoneTb.setBrokerGroup(groupContext);
        groupContext = brokerGroup = Constants.KURORO_DEFAULT_BROKER_GROUPNAME;
      } else {
        brokerGroup = zoneTb.getBrokerGroup();
      }
      brokerGroupPath = zoneRootPath + Constants.ZONE_BROKER + Constants.SEPARATOR
          + brokerGroup + Constants.ZONE_BROKER_CONSUMER;

      String routerManagerKey = topicName + brokerGroup;
      if (groupMap.get(routerManagerKey) != null) {
        routerManager = groupMap.get(routerManagerKey);
      } else {
        routerManager = new BrokerGroupRouteManager(brokerGroupPath, topicName,
            InternalPropKey.BALANCER_NAME_CONSISTENTHASH, groupContext);
        groupMap.put(routerManagerKey, routerManager);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    InetSocketAddress consumerAddress = getConsumerAddress();
    String key = topicName + consumerAddress.getAddress() + ":"
        + consumerAddress.getPort();

    if (connectionPool.containsKey(key)) {
      LinkedBlockingQueue<Consumer> pool = connectionPool.get(key);
      if (!pool.isEmpty()) {
        if (pool.size() >= config.getMaxConnectionCount()) {

          try {
            Consumer consumer = pool.poll();
            if (consumer != null) {
              pool.put(consumer);
              LOG.warn("Consumer connection reaches the max count:"
                  + config.getMaxConnectionCount()
                  + ",reuse an instance "
                  + consumer.getRemoteAddress());
              return consumer;
            }
          } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }

    Consumer consumer = new ConsumerImpl(dest, consumerId, config, consumerAddress,
        routerManager,
        ConsumerFactoryImpl.connectionPool);
    String consumerPath = zoneRootPath + Constants.KURORO_CONSUMERS_PATH;
    if (!KuroroUtil.isBlankString(poolId)) {
      String consumerData = topicName + "#" + consumerId + "#" + poolId;
      registerConsumerMeta(consumerPath, consumerData);
    } else {
      LOG.warn("poolId is null ! please set poolId Name !");
    }
    consumerMap.put(consumer.toString(), consumer);
    return consumer;
  }

  //init idc consumer
  private Consumer initIdcConsumer(Destination dest, String consumerId,
      ConsumerConfig config) {

    String brokerGroupPath = null;
    String brokerGroup = null;
    String topicName = dest.getName();
    TopicConfigDataMeta topicConfig = null;
    String groupContext = null;
    String idcZkTopicPath = idcZkRoot + Constants.ZONE_TOPIC + Constants.SEPARATOR + topicName;
    if (zkClient.exists(idcZkTopicPath)) {
      topicConfig = zkClient.readData(idcZkTopicPath);
      if (topicConfig == null) {
        try {
          throw new Exception("Can not find topic[" + topicName
              + "] register info, please check or contact MQ administrator.");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    groupContext = topicConfig.getBrokerGroup();
    if (KuroroUtil.isBlankString(groupContext)) {
      topicConfig.setBrokerGroup(groupContext);
      groupContext = brokerGroup = Constants.KURORO_DEFAULT_BROKER_GROUPNAME;
    } else {
      brokerGroup = topicConfig.getBrokerGroup();
    }
    brokerGroupPath = idcZkRoot + Constants.ZONE_BROKER + Constants.SEPARATOR
        + brokerGroup + Constants.ZONE_BROKER_CONSUMER;

    String routerManagerKey = topicName + brokerGroup;
    if (groupMap.get(routerManagerKey) != null) {
      routerManager = groupMap.get(routerManagerKey);
    } else {
      routerManager = new BrokerGroupRouteManager(brokerGroupPath, topicName,
          InternalPropKey.BALANCER_NAME_CONSISTENTHASH, groupContext);
      groupMap.put(routerManagerKey, routerManager);
    }

    InetSocketAddress consumerAddress = getConsumerAddress();
    String key = topicName + consumerAddress.getAddress() + ":"
        + consumerAddress.getPort();
    if (connectionPool.containsKey(key)) {
      LinkedBlockingQueue<Consumer> pool = connectionPool.get(key);
      if (!pool.isEmpty()) {
        if (pool.size() >= config.getMaxConnectionCount()) {

          try {
            Consumer consumer = pool.poll();
            if (consumer != null) {
              pool.put(consumer);
              LOG.warn("Consumer connection reaches the max count:"
                  + config.getMaxConnectionCount()
                  + ",reuse an instance "
                  + consumer.getRemoteAddress());
              return consumer;
            }
          } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
          }

        }
      }
    }

    Consumer consumer = new ConsumerImpl(dest, consumerId, config, consumerAddress,
        routerManager,
        ConsumerFactoryImpl.connectionPool);
    String consumerPath = idcZkRoot + Constants.KURORO_CONSUMERS_PATH;
    if (!KuroroUtil.isBlankString(poolId)) {
      String consumerData = topicName + "#" + consumerId + "#" + poolId;
      registerConsumerMeta(consumerPath, consumerData);
    }
    consumerMap.put(consumer.toString(), consumer);
    return consumer;
  }

  private InetSocketAddress getConsumerAddress() {
    HostInfo hi = routerManager.route();
    if (hi == null) {
      throw new NullPointerException(
          "MQ consumer server not found, please contact us for help.");
    }
    return new InetSocketAddress(hi.getHost(), hi.getPort());
  }

  private void registerConsumerMeta(String consumerPath, String data) {
    String path = consumerPath + Constants.SEPARATOR + data;
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true);
    }
  }
}

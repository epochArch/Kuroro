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

package com.epocharch.kuroro.producer.impl;

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.producer.ProducerService;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.message.Destination;
import com.epocharch.kuroro.producer.Producer;
import com.epocharch.kuroro.producer.ProducerConfig;
import com.epocharch.kuroro.producer.ProducerFactory;
import com.epocharch.kuroro.producer.netty.BrokerGroupRouteManagerFactory;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerFactoryImpl implements ProducerFactory {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ProducerFactoryImpl.class);
  private static ProducerFactoryImpl instance;
  private ProducerService producerService;
  private ZkClient _zkClient = null;
  private String zoneIDCName = null;
  private String localZoneName = null;
  private String zoneRootPath = null;
  private Destination destination = null;
  private ProducerConfig config = null;
  private String idcZkroot = null;
  private boolean isIdcProduction = false;
  private String poolId = null;

  private ProducerFactoryImpl() {
    init();
  }

  public static synchronized ProducerFactoryImpl getInstance() {
    if (instance == null) {
      instance = new ProducerFactoryImpl();
    }
    return instance;
  }

  private void init() {
    producerService = new ProducerServiceImpl();
  }

  @Override
  public Producer createProducer(Destination destination) {
    return createProducer(destination, new ProducerConfig());
  }

  @Override
  public Producer createProducer(Destination destination,
      ProducerConfig config) {
    if (destination == null) {
      throw new IllegalArgumentException("Destination can not be null");
    }
    this.destination = destination;
    this.config = config;
    isIdcProduction = KuroroUtil.isIdcProduction();
    System.setProperty(InternalPropKey.POOL_LEVEL, String.valueOf(isIdcProduction));
    poolId = KuroroUtil.poolIdName();
    _zkClient = KuroroZkUtil.initLocalMqZk();

    if (isIdcProduction) {
      return createIDCProducer();
    } else {

      return createZoneProducer();
    }
  }

  private ProducerImpl createZoneProducer() {

    KuroroZkUtil.initClientIDCMetas();
    zoneIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
    localZoneName = System.getProperty(InternalPropKey.ZONE_LOCALZONE);
    zoneRootPath = System.getProperty(InternalPropKey.ZONE_KURORO_ZK_ROOT_PATH)
        + Constants.SEPARATOR + localZoneName;

    ProducerImpl producerImpl = null;
    producerImpl = new ProducerImpl(destination, config, producerService);
    LOGGER.info("New producer:[TopicName=" + destination.getName() + "; "
        + producerImpl.getProducerConfig().toString() + "]");

    String zoneParentPath = zoneRootPath + Constants.ZONE_TOPIC;
    if (!_zkClient.exists(zoneParentPath)) {
      throw new IllegalArgumentException(
          "Topic don't registered, please notify MQ Owner ");
    }

    StringBuilder zoneChildPathBuilder = new StringBuilder(zoneParentPath).append(
        Constants.SEPARATOR).append(destination.getName());
    if (!_zkClient.exists(zoneChildPathBuilder.toString())) {
      LOGGER.warn("Zk can not find this topic {}",
          destination.getName() + "===path===" + zoneChildPathBuilder);
    }

    String brokerZkPath = zoneRootPath + Constants.ZONE_BROKER;
    TopicConfigDataMeta topicConfig = null;
    String topicGroup = null;
    if (_zkClient.exists(zoneChildPathBuilder.toString())) {
      topicConfig = _zkClient.readData(zoneChildPathBuilder.toString());
      topicGroup = topicConfig.getBrokerGroup();
      if (null == topicConfig) {
        throw new IllegalStateException(
            "Zk can not read the data of this topic: " + destination.getName());
      }
    } else {
      throw new IllegalStateException(
          "can not find topic : " + destination.getName() + " from zookeeper! ");
    }

    if (KuroroUtil.isBlankString(topicGroup)) {
      topicGroup = Constants.KURORO_DEFAULT_BROKER_GROUPNAME;
    }

    BrokerGroupRouteManagerFactory.getInstacne().createBrokerGroupRouteManager(topicGroup,
        topicConfig.getTopicName(),
        brokerZkPath + Constants.SEPARATOR + topicGroup + Constants.ZONE_BROKER_PRODUCER);

    if (topicConfig.isSendFlag() == null) {
      producerImpl.getProducerConfig().setSendFlag(true);
    } else {
      producerImpl.getProducerConfig().setSendFlag(topicConfig.isSendFlag());
    }
    //observe the data of this zk path
    observeBrokerPathChanged(zoneChildPathBuilder.toString());

    String zoneProducerPath = zoneRootPath + Constants.KURORO_PRODUCER_PATH;
    if (!KuroroUtil.isBlankString(poolId)) {
      String producerData = destination.getName() + "#" + poolId;
      registerProducerMeta(zoneProducerPath, producerData);
    } else {
      LOGGER.warn("=== current poolId is null! please set poolId ! ===");
    }

    return producerImpl;
  }

  //idc production init producer connection
  private ProducerImpl createIDCProducer() {

    KuroroZkUtil.initBrokerIDCMetas();
    idcZkroot = System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH);
    String idcZkTopicPath = idcZkroot + Constants.ZONE_TOPIC;
    String brokerZkPath = idcZkroot + Constants.ZONE_BROKER;
    TopicConfigDataMeta topicConfig = null;
    String topicGroup = null;

    ProducerImpl producerImpl = null;
    producerImpl = new ProducerImpl(destination, config, producerService);
    LOGGER.info("New producer:[TopicName=" + destination.getName() + "; "
        + producerImpl.getProducerConfig().toString() + "]");
    if (!_zkClient.exists(idcZkTopicPath)) {
      throw new IllegalArgumentException(
          "Topic don't registered , please notify kuroro owner!" + idcZkTopicPath);
    }
    StringBuilder childPathBuilder = new StringBuilder(idcZkTopicPath).append(
        Constants.SEPARATOR).append(destination.getName());
    if (!_zkClient.exists(childPathBuilder.toString())) {
      throw new IllegalStateException(
          "idc Zk can not find this topic: " + destination.getName() + "=== path:"
              + childPathBuilder);
    }
    topicConfig = _zkClient.readData(childPathBuilder.toString());
    if (null == topicConfig) {
      throw new IllegalStateException(
          "Zk can not read the data of this topic: " + destination.getName());
    }
    topicGroup = topicConfig.getBrokerGroup();
    if (KuroroUtil.isBlankString(topicGroup)) {
      topicGroup = Constants.KURORO_DEFAULT_BROKER_GROUPNAME;
    }
    BrokerGroupRouteManagerFactory.getInstacne().createBrokerGroupRouteManager(topicGroup,
        topicConfig.getTopicName(),
        brokerZkPath + Constants.SEPARATOR + topicGroup + Constants.ZONE_BROKER_PRODUCER);
    if (topicConfig.isSendFlag() == null) {
      producerImpl.getProducerConfig().setSendFlag(true);
    } else {
      producerImpl.getProducerConfig().setSendFlag(topicConfig.isSendFlag());
    }

    //observe the data of this zk path
    if (topicConfig != null) {
      observeBrokerPathChanged(childPathBuilder.toString());
    }
    String zoneProducerPath = idcZkroot + Constants.KURORO_PRODUCER_PATH;
    if (!KuroroUtil.isBlankString(poolId)) {
      String producerData = destination.getName() + "#" + poolId;
      registerProducerMeta(zoneProducerPath, producerData);
    } else {
      LOGGER.warn("=== current poolId is null! please set poolId ! ===");
    }
    return producerImpl;
  }

  private void observeBrokerPathChanged(final String basePath) {
    _zkClient.subscribeDataChanges(basePath, new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data)
          throws Exception {

        TopicConfigDataMeta topicConfigData = (TopicConfigDataMeta) data;
        if (StringUtils.isBlank(topicConfigData.getBrokerGroup())) {
          return;
        }

        String brokerGroup = topicConfigData.getBrokerGroup();
        String topicName = topicConfigData.getTopicName();
        //create new ProducerRouteManager if need
        String producerPath = null;
        if (isIdcProduction) {
          producerPath = idcZkroot + Constants.ZONE_BROKER + Constants.SEPARATOR + brokerGroup
              + Constants.ZONE_BROKER_PRODUCER;
        } else {
          producerPath = zoneRootPath + Constants.ZONE_BROKER + Constants.SEPARATOR + brokerGroup
              + Constants.ZONE_BROKER_PRODUCER;
        }
        BrokerGroupRouteManagerFactory.getInstacne()
            .createBrokerGroupRouteManager(brokerGroup, topicName,
                producerPath);
        //banding brokerGroup and topic, and close old brokerGroup connection if need
        BrokerGroupRouteManagerFactory.getInstacne()
            .bandingDestinationToBrokerGroup(brokerGroup, topicName);
      }

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {

      }
    });
  }

  private void registerProducerMeta(String producerPath, String data) {
    String path = producerPath + Constants.SEPARATOR + data;
    if (!_zkClient.exists(path)) {
      _zkClient.createPersistent(path, true);
    }
  }
}

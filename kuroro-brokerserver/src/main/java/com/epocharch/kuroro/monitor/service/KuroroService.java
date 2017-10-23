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

package com.epocharch.kuroro.monitor.service;

import com.epocharch.kuroro.monitor.dto.MessageView;
import com.epocharch.kuroro.monitor.jmx.MessageRegEntity;
import java.util.List;
import java.util.Map;

public interface KuroroService {

  String getAliveHost();

  String getAllMessageReg();

  void insertMessageReg(MessageRegEntity jsonData);

  void deleteMessageReg(Integer id);

  void updateMessageReg(MessageRegEntity jsonData);

  String getAllTopic();

  String deleteTopic(String topic);

  String saveTopic(String jsonData);

  String editTopic(String jsonData);

  String getTopic(String topic);

  String getTransferData(String topic);

  String saveTransferData(String data);

  String deleteTransferData(String topicRegionData);

  String getZones();

  String getBrokerGroups(String zone);

  String getTransferGroups(String zone);

  String listConsumerStatusTree(String group);

  String getConsumerStatus(String nodeid) throws Exception;

  String getCompensatorStatus(String nodeid);

  String invokeJmxMethod(String nodeid);

  String listCompensatorTree(String group);

  String getSyncTopicDestination();

  String updateTransferDataStatus(String data);

  String modifyTransferDataStatus(String data);

  String getMongoGroupList(String zone);

  String batchEditTopicMongo(String jsonData);

  String mongoQuery(String jsonData);

  Map<String, Object> getLocalIdcZones();

  Map<String, Object> getLocalZoneName();

  Map<String, Object> getIDCAllZones();

  Map<String, Object> getAllZoneDic(String excludeZone);

  Map<String, Object> getAllIdcDic(String excludeZone);

  String getLocalIdcName();

  String syncIdcTopics(String json);

  String writeTopicToZk(String topics);

  String getSyncZoneTopicDestination();

  String syncZoneTopics(String json);

  String getAllIdcName(String idc);

  void updateRemoteProducerServerIpListToLocalIdcZk(String p);

  String getProducerServerIpList();

  String deleteTransferTopic(String transferTopicPath);

  String createTransferTopic(String jsonData);

  String messageTrackQuery(String jsonData);

  List<MessageView> queryMessageDetail(String jsonData);
}

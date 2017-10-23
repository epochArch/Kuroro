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

package com.epocharch.kuroro.monitor.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.epocharch.common.config.PropertiesContainer;
import com.epocharch.common.zone.Zone;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InConstants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.config.impl.TopicZoneReplication;
import com.epocharch.kuroro.common.inner.config.impl.TransferConfigDataMeta;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.epocharch.kuroro.common.inner.util.NameCheckUtil;
import com.epocharch.kuroro.common.inner.util.ZipUtil;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.protocol.CachedData;
import com.epocharch.kuroro.common.protocol.Transcoder;
import com.epocharch.kuroro.common.protocol.hessian.HessianTranscoder;
import com.epocharch.kuroro.common.protocol.json.JsonBinder;
import com.epocharch.kuroro.monitor.common.MonitorHttpClientUtil;
import com.epocharch.kuroro.monitor.dao.impl.BaseMybatisDAOImpl;
import com.epocharch.kuroro.monitor.dto.MessageView;
import com.epocharch.kuroro.monitor.jmx.KuroroJmxService;
import com.epocharch.kuroro.monitor.jmx.MessageRegEntity;
import com.epocharch.kuroro.monitor.service.KuroroService;
import com.epocharch.kuroro.monitor.support.AjaxRequest;
import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import com.epocharch.zkclient.ZkClient;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.collections.keyvalue.DefaultKeyValue;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;

public class KuroroServiceImpl implements KuroroService {

  public static final String KURORO_TOPIC_PATH = MonitorZkUtil.IDCZkTopicPath;
  public static final Integer PORT = 3997;
  private static final Logger logger = Logger.getLogger(KuroroServiceImpl.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
	  mapper.setSerializationInclusion(Include.NON_EMPTY);
    mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    //make all member fields serializable without further annotations, instead of just public fields (default setting).
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
  }

  private final String MSG_OK = "OK";
  private final String MSG_FAIL = "fail";
  private final String CONTENT = "c";
  private final String VERSION = "v";
  private final String SHA1 = "s";
  private final String GENERATED_TIME = "gt";
  private final String PROPERTIES = "p";
  private final String INTERNAL_PROPERTIES = "_p";
  private final String TYPE = "t";
  private final String SOURCE_IP = "si";
  private final String PROTOCOLTYPE = "pt";
  private final String CREATETIME = "ct";
  private final String ID = "nid";
  private final int retry = 5;
  private BaseMybatisDAOImpl analyseMyIbaitsDAO;


  public void setAnalyseMyIbaitsDAO(
      BaseMybatisDAOImpl analyseMyIbaitsDAO) {
    this.analyseMyIbaitsDAO = analyseMyIbaitsDAO;
  }


  public String getAliveHost() {
    String result = "";
    try {
      List<String> producerHostList = MonitorZkUtil.getLocalIDCZk()
          .getChildren(MonitorZkUtil.IDCZkConsumerServerDefaultPath);
      result = mapper.writeValueAsString(producerHostList);

    } catch (Exception e) {
      result = new StringBuilder("errorMsg").append(":").append(e.getMessage()).toString();
    }

    return result;
  }

  @Override
  public String getAllMessageReg() {
    List<MessageRegEntity> list = analyseMyIbaitsDAO
        .queryForList("monitor_jmx_message.getAllMessageReg", MessageRegEntity.class);
    String result = null;
    try {
      return mapper.writeValueAsString(list);
    } catch (Exception e) {
      result = new StringBuilder("errorMsg").append(":").append(e.getMessage()).toString();
    }
    return result;
  }

  @Override
  public void insertMessageReg(MessageRegEntity jsonData) {
    analyseMyIbaitsDAO.insert("monitor_jmx_message.insert", jsonData);
  }

  @Override
  public void deleteMessageReg(Integer id) {
    analyseMyIbaitsDAO.delete("monitor_jmx_message.deleteById", id);
  }

  @Override
  public void updateMessageReg(MessageRegEntity jsonData) {
    analyseMyIbaitsDAO.update("monitor_jmx_message.update", jsonData);
  }

  @Override
  public String getAllTopic() {
    String result = "";
    try {
      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      if (zkClient.exists(KURORO_TOPIC_PATH)) {
        List<Object> objList = new ArrayList<Object>();
        List<String> childList = zkClient.getChildren(KURORO_TOPIC_PATH);
        if (childList == null) {
          return result;
        }
        for (String child : childList) {
          String childPath = KuroroUtil.getChildFullPath(KURORO_TOPIC_PATH, child);
          if (zkClient.exists(childPath)) {
            TopicConfigDataMeta meta = zkClient.readData(childPath);
            if (meta.getMongoGroup() == null) {
              meta.setMongoGroup(String.valueOf(meta.getReliability()));
            }
            Object obj = (Object) meta;
            if (obj != null) {
              HashMap<String, Object> map = JSON
                  .parseObject(mapper.writeValueAsBytes(obj), HashMap.class);
              List<String> idcs = zkClient.getChildren(childPath);
              map.put("midc", idcs);
              objList.add(map);
            }
          }
        }

        if (objList.size() > 0) {
          result = mapper.writeValueAsString(objList);
        }
      }
    } catch (Exception e) {
      result = new StringBuilder("errorMsg").append(":").append(e.getMessage()).toString();
    }
    return result;
  }

  @Override
  public String deleteTopic(String topic) {
    ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
    String childPath = KuroroUtil.getChildFullPath(KURORO_TOPIC_PATH, topic);
    if (zkClient.exists(childPath)) {
      zkClient.delete(childPath);
    }
    Set<String> localIdcZones = KuroroZkUtil.localIdcZones();
    for (String zone : localIdcZones) {
      ZkClient zoneClient = MonitorZkUtil.getIDCZK(zone);
      String zoneTopicPath =
          MonitorZkUtil.IDCZoneZKPath + Constants.SEPARATOR + zone + Constants.ZONE_TOPIC;
      String zoneChild = KuroroUtil.getChildFullPath(zoneTopicPath, topic);
      if (zoneClient.exists(zoneChild)) {
        zoneClient.delete(zoneChild);
        logger.warn("delete zone === " + zone + " topicPath === " + zoneChild);
      }
    }

    return MSG_OK;
  }

  @Override
  public String getTopic(String topic) {
    String result = "";
    try {
      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      String childPath = KuroroUtil.getChildFullPath(KURORO_TOPIC_PATH, topic);
      if (zkClient.exists(childPath)) {
        TopicConfigDataMeta data = zkClient.readData(childPath);

        if (data != null) {
          if (data.getMongoGroup() == null) {
            data.setMongoGroup(String.valueOf(data.getReliability()));
          }
          if (data.isSendFlag() == null) {
            data.setSendFlag(true);
          }
          if (data.isConsumerFlowControl() == null) {
            data.setConsumerFlowControl(true);
          }
          result = mapper.writeValueAsString(data);
        }
      }
    } catch (Exception e) {
      result = new StringBuilder("errorMsg").append(":").append(e.getMessage()).toString();
    }
    return result;
  }

  @Override
  public String saveTopic(String jsonData) {
    StringBuffer ret = new StringBuffer();
    try {
      TopicDataMeta data = mapper.readValue(jsonData, TopicDataMeta.class);

      String idcs = data.getIdc();
      if (idcs == null || idcs.isEmpty()) {
        return "所选 IDC 不合法";
      }
      String brokerGroup = data.getBrokerGroup();

      for (String idc : StringUtils.split(idcs, ",")) {

        if (!idc.equals(System.getProperty(InternalPropKey.ZONE_IDCNAME))) {
          data.setIdc(idc);
          String destJsonData = JSONObject.toJSONString(data);
          AjaxRequest ajaxRequest = new AjaxRequest();
          ajaxRequest.setS("kuroroService");
          ajaxRequest.setM("saveTopic");
          ajaxRequest.setI(idc);
          ajaxRequest.setP(destJsonData);

          String result = (String) requestIdcData(ajaxRequest, idc);
          if (!result.equals(MSG_OK)) {
            ret.append("create topic fail in " + idc + "<br/>");
          }
          continue;
        }

        if (MonitorZkUtil.getAllIdcName().contains(idc)) {
          ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
          if (!zkClient.exists(KURORO_TOPIC_PATH)) {
            zkClient.createPersistent(KURORO_TOPIC_PATH, true);
          }
          String childPath = KuroroUtil.getChildFullPath(KURORO_TOPIC_PATH, data.getTopicName());

          if (zkClient.exists(childPath)) {
            ret.append("Topic already exists in " + idc + "<br/>");
            continue;
          }

          TopicConfigDataMeta cd = new TopicConfigDataMeta();
          cd.setTopicName(data.getTopicName());
          cd.setPoolName(data.getPoolName());
          cd.setOwnerEmail(data.getOwnerEmail());
          cd.setOwnerName(data.getOwnerName());
          cd.setOwnerPhone(data.getOwnerPhone());
          cd.setType(data.getType());
          cd.setBrokerGroup(brokerGroup);
          cd.setAckCappedMaxDocNum(data.getAckCappedMaxDocNum());
          cd.setAckCappedSize(data.getAckCappedSize());
          cd.setMessageCappedMaxDocNum(data.getMessageCappedMaxDocNum());
          cd.setMessageCappedSize(data.getMessageCappedSize());
          cd.setLevel(data.getLevel());
          cd.setCompensate(data.isCompensate());
          cd.setSendFlag(data.isSendFlag());
          cd.setConsumerFlowControl(data.isConsumerFlowControl());
          cd.setMongoGroup(data.getMongoGroup());
          cd.setTimeStamp(System.currentTimeMillis());
          cd.setComment(data.getComment());
          cd.setFlowSize(data.getFlowSize());

          String mongoGroup = data.getMongoGroup();

          List<String> mongoList = zkClient.getChildren(MonitorZkUtil.IDCZkMongoPath);
          Map<String, String> mongoMap = new HashMap<String, String>();
          for (String mongo : mongoList) {
            String addr = zkClient
                .readData(MonitorZkUtil.IDCZkMongoPath + Constants.SEPARATOR + mongo);
            mongoMap.put(mongo, addr);
          }

          List<String> mongos = new ArrayList<String>();
          if (mongoGroup != null) {
            if (mongoMap.containsKey(mongoGroup)) {
              mongos.add(mongoMap.get(mongoGroup));
            } else {
              for (String key : mongoMap.keySet()) {
                cd.setMongoGroup(key);
                mongos.add(mongoMap.get(key));
                break;
              }
            }
            cd.setReplicationSetList(mongos);
          } else {
            return "mongo集群  不能为空...";
          }
          zkClient.createPersistent(childPath, cd);

          //automatic topic zone zk path and old topic path register
          String zoneTopic = MonitorZkUtil.IDCZkKuroroRootPath + Constants.ZONES;
          for (String zoneName : KuroroZkUtil.localIdcAllZones()) {
            if (KuroroZkUtil.localZone().equals(zoneName)
                && KuroroZkUtil.localIdcAllZones().size() > 1) {
              continue;
            }
            ZkClient zoneZkClient = KuroroZkUtil.initMqZK(zoneName);

            if (zoneZkClient != null) {
              String _zoneTopicPath =
                  zoneTopic + Constants.SEPARATOR + zoneName + Constants.ZONE_TOPIC
                      + Constants.SEPARATOR + data
                      .getTopicName();
              if (!zoneZkClient.exists(_zoneTopicPath)) {
                zoneZkClient.createPersistent(_zoneTopicPath, true);
                zoneZkClient.writeData(_zoneTopicPath, cd);
              } else {
                zoneZkClient.writeData(_zoneTopicPath, cd);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }

    return ret.length() == 0 ? MSG_OK : ret.toString();
  }

  @Override
  public String editTopic(String jsonData) {
    try {
      TopicDataMeta data = mapper.readValue(jsonData, TopicDataMeta.class);
      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      if (!zkClient.exists(KURORO_TOPIC_PATH)) {
        zkClient.createPersistent(KURORO_TOPIC_PATH);
      }
      String childPath = KuroroUtil.getChildFullPath(KURORO_TOPIC_PATH, data.getTopicName());

      if (!zkClient.exists(childPath)) {
        return "Topic not exist";
      }
      String brokerGroup = data.getBrokerGroup();

      TopicConfigDataMeta cd = new TopicConfigDataMeta();
      cd.setTopicName(data.getTopicName());
      cd.setPoolName(data.getPoolName());
      cd.setOwnerEmail(data.getOwnerEmail());
      cd.setOwnerName(data.getOwnerName());
      cd.setOwnerPhone(data.getOwnerPhone());
      //cd.setAlarmParams(data.getAlarmParams());
      cd.setType(data.getType());
      cd.setBrokerGroup(brokerGroup);
      cd.setAckCappedMaxDocNum(data.getAckCappedMaxDocNum());
      cd.setAckCappedSize(data.getAckCappedSize());
      cd.setMessageCappedMaxDocNum(data.getMessageCappedMaxDocNum());
      cd.setMessageCappedSize(data.getMessageCappedSize());
      cd.setLevel(data.getLevel());
      cd.setCompensate(data.isCompensate());
      cd.setSendFlag(data.isSendFlag());
      cd.setConsumerFlowControl(data.isConsumerFlowControl());
      cd.setMongoGroup(data.getMongoGroup());
      cd.setComment(data.getComment());
      cd.setFlowSize(data.getFlowSize());

      TopicConfigDataMeta meta = zkClient.readData(childPath);
      if (meta != null) {
        cd.setTimeStamp(meta.getTimeStamp());
        List<String> mongoList = zkClient.getChildren(MonitorZkUtil.IDCZkMongoPath);
        Map<String, String> mongoMap = new HashMap<String, String>();
        for (String mongo : mongoList) {
          String addr = zkClient
              .readData(MonitorZkUtil.IDCZkMongoPath + Constants.SEPARATOR + mongo);
          mongoMap.put(mongo, addr);
        }

        String mongoGroup = data.getMongoGroup();

        List<String> mongos = new ArrayList<String>();
        if (mongoGroup != null && mongoMap.containsKey(mongoGroup)) {
          mongos.add(mongoMap.get(mongoGroup));
          cd.setReplicationSetList(mongos);
        } else {
          cd.setMongoGroup(meta.getMongoGroup());
          cd.setReplicationSetList(meta.getReplicationSetList());
        }
      } else {
        return "Topic not exist";
      }

      zkClient.writeData(childPath, cd);

      //automatic topic zone zk path and old topic path register
      String zoneTopic = MonitorZkUtil.IDCZkKuroroRootPath + Constants.ZONES;
      for (String zoneName : KuroroZkUtil.localIdcAllZones()) {
        if (KuroroZkUtil.localZone().equals(zoneName)
            && KuroroZkUtil.localIdcAllZones().size() > 1) {
          continue;
        }
        ZkClient zoneZkClient = KuroroZkUtil.initMqZK(zoneName);

        if (zoneZkClient != null) {
          String _zoneTopicPath =
              zoneTopic + Constants.SEPARATOR + zoneName + Constants.ZONE_TOPIC
                  + Constants.SEPARATOR + data.getTopicName();
          if (!zoneZkClient.exists(_zoneTopicPath)) {
            zoneZkClient.createPersistent(_zoneTopicPath, true);
            zoneZkClient.writeData(_zoneTopicPath, cd);
          } else {
            zoneZkClient.writeData(_zoneTopicPath, cd);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return MSG_OK;
  }

  @Override
  public String getTransferData(String topic) {
    List<TopicZoneReplication> dataList = new ArrayList<TopicZoneReplication>();
    String childPath = MonitorZkUtil.IDCZkTopicPath + Constants.SEPARATOR + topic;
    try {
      ZkClient zkcli = MonitorZkUtil.getLocalIDCZk();
      List<String> zoneList = zkcli.getChildren(childPath);
      if (zoneList != null) {
        for (String zone : zoneList) {
          String subPath = childPath + Constants.SEPARATOR + zone;
          Object repMeta = zkcli.readData(subPath, true);
          if (repMeta instanceof TopicZoneReplication) {
            TopicZoneReplication data = (TopicZoneReplication) repMeta;
            dataList.add(data);
          } else if (repMeta instanceof TransferConfigDataMeta) {
            TransferConfigDataMeta oldMeta = (TransferConfigDataMeta) repMeta;
            TopicZoneReplication oldTzr = new TopicZoneReplication();
            oldTzr.setActiveFlag(true);
            oldTzr.setConsumerId(oldMeta.getRegionName() + "_trans");
            oldTzr.setTopic(oldMeta.getTopicName());
            oldTzr.setDestTopic(oldMeta.getTopicName());
            oldTzr.setSync(!oldMeta.getIsAsync());
            oldTzr.setFilter(oldMeta.getMessageFilter());
            String destZone = null;
            if ("nanhui".equals(oldMeta.getRegionName())) {
              destZone = "shanghai_nh";
            } else if ("jinqiao".equals(oldMeta.getRegionName())) {
              destZone = "shanghai_jq";
            }
            oldTzr.setDestZone(destZone);
            dataList.add(oldTzr);
          }
        }
        if (dataList.size() > 0) {
          return mapper.writeValueAsString(dataList);
        }
      }
    } catch (JsonGenerationException e) {
      return e.getMessage();
    } catch (JsonMappingException e) {
      return e.getMessage();
    } catch (IOException e) {
      return e.getMessage();
    }
    return null;
  }

  @Override
  public String saveTransferData(String jsonData) {
    try {
      List<TopicZoneReplication> dataList = mapper
          .readValue(jsonData, mapper.getTypeFactory()
              .constructCollectionType(List.class, TopicZoneReplication.class));
      for (TopicZoneReplication data : dataList) {
        if (!NameCheckUtil.isTopicNameValid(data.getDestTopic())) {
          return "topic[" + data.getDestTopic() + "]存在无效字符";
        }
        if (!NameCheckUtil.isConsumerIdValid(data.getConsumerId())) {
          return "ConsumerId[" + data.getConsumerId() + "]存在无效字符";
        }
      }
      for (TopicZoneReplication data : dataList) {
        StringBuilder topicCrossZoneRepPath = new StringBuilder(MonitorZkUtil.IDCZkTopicPath);
        topicCrossZoneRepPath.append(Constants.SEPARATOR).append(data.getTopic());
        topicCrossZoneRepPath.append(Constants.SEPARATOR).append(data.getZKNodePath());
        ZkClient zkcli = MonitorZkUtil.getLocalIDCZk();
        if (zkcli.exists(topicCrossZoneRepPath.toString())) {
          MonitorZkUtil.getLocalIDCZk().writeData(topicCrossZoneRepPath.toString(), data);
        } else {
          MonitorZkUtil.getLocalIDCZk().createPersistent(topicCrossZoneRepPath.toString(), data);
        }
      }
    } catch (Exception e) {
      return e.getMessage();
    }
    return MSG_OK;
  }

  @Override
  public String getZones() {
    Set<String> names = MonitorZkUtil.getAllIdcName();
    List<Map<String, String>> ret = new ArrayList<Map<String, String>>();
    for (String name : names) {
      if (KuroroUtil.isBlankString(name)) {
        continue;
      }
      Map<String, String> map = new HashMap<String, String>();
      map.put("text", name);
      map.put("value", name);
      ret.add(map);
    }
    return JSON.toJSONString(ret);
  }

  @Override
  public String getBrokerGroups(String idc) {
    //根据zone获取ZkClient
    ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();

    List<String> groups = zkClient.getChildren(MonitorZkUtil.IDCZkBrokerGroupPath);
    List<Map<String, String>> ret = new ArrayList<Map<String, String>>();
    for (String name : groups) {
      Map<String, String> map = new HashMap<String, String>();
      map.put("text", name);
      map.put("value", name);
      ret.add(map);
    }
    return JSON.toJSONString(ret);
  }

  @Override
  public String getTransferGroups(String idc) {
    //根据zone获取ZkClient
    ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();

    List<String> groups = zkClient.getChildren(MonitorZkUtil.IDCZkTransferGroupPath);
    List<Map<String, String>> ret = new ArrayList<Map<String, String>>();
    if (groups != null && !groups.isEmpty()) {
      for (String name : groups) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("text", name);
        map.put("value", name);
        ret.add(map);
      }
    }
    return JSON.toJSONString(ret);
  }

  public String listConsumerStatusTree(String group) {
    List<StatusNode> tree = new ArrayList<StatusNode>();
    try {
      Map<String, List<String>> hostsMap = getConsumerHosts();

      final Map<String, KuroroJmxService> serviceMap = new HashMap<String, KuroroJmxService>();
      int hostSize = 0;

      for (List<String> ls : hostsMap.values()) {
        if (ls.size() > 0) {
          hostSize = ls.size();
        }
        for (String h : ls) {
          serviceMap.put(h, KuroroJmxService.getKuroroJmxClientInstance(h, PORT));
        }
      }

      if ("broker".equals(group)) {
        for (String groupName : hostsMap.keySet()) {

          StatusNode root = new StatusNode();
          root.cls = StatusNode.CLS_FOLDER;
          root.id = groupName;
          root.leaf = false;
          root.text = groupName;
          tree.add(root);

          List<StatusNode> hostChild = new ArrayList<StatusNode>();
          List<String> hosts = hostsMap.get(root.id);
          for (String host : hosts) {
            StatusNode node = new StatusNode();
            node.cls = StatusNode.CLS_FOLDER;
            node.id = host + '@' + root.id;
            node.leaf = false;
            node.text = host;
            hostChild.add(node);
          }
          root.children = hostChild;

        }

        final CountDownLatch latch = new CountDownLatch(hostSize);

        for (StatusNode n : tree) {
          List<StatusNode> ch = n.children;
          for (final StatusNode node : ch) {
            KuroroJmxService service = serviceMap.get(node.id.split("@")[0]);
            Map<String, List<String>> consumers = service.getAllConsumers();
            List<StatusNode> children = new ArrayList<StatusNode>();
            Iterator<Map.Entry<String, List<String>>> it = consumers.entrySet().iterator();
            while (it.hasNext()) {
              Map.Entry<String, List<String>> entry = it.next();

              String topicName = entry.getKey();
              List<String> consumerIds = entry.getValue();

              StatusNode child = new StatusNode();
              child.id = topicName + '@' + node.id;
              child.cls = StatusNode.CLS_FOLDER;
              child.leaf = false;
              child.text = topicName;
              children.add(child);

              List<StatusNode> leaves = new ArrayList<StatusNode>();
              for (String consumerId : consumerIds) {
                StatusNode leaf = new StatusNode();
                leaf.id = consumerId + '@' + child.id;
                leaf.cls = StatusNode.CLS_LEAF;
                leaf.leaf = true;
                leaf.text = consumerId;
                leaves.add(leaf);
              }
              child.children = leaves;
            }
            node.children = children;
            latch.countDown();
          }
        }
        latch.await();

      } else {
        if ("topic".equals(group)) {
          //topic,consumerid,host
          Map<String, Map<String, List<String>>> root = new ConcurrentHashMap<String, Map<String, List<String>>>();
          final CountDownLatch latch = new CountDownLatch(hostSize);

          final Map<String, Map<String, List<String>>> results = new ConcurrentHashMap<String, Map<String, List<String>>>();
          List<String> hosts = new ArrayList<String>();
          for (List<String> ls : hostsMap.values()) {
            if (ls != null && ls.size() > 0) {
              hosts.addAll(ls);
            }
          }
          for (final String host : hosts) {
            try {
              KuroroJmxService service = serviceMap.get(host);
              Map<String, List<String>> consumers = service.getAllConsumers();
              results.put(host, consumers);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
          for (Map.Entry<String, Map<String, List<String>>> resultEntry : results.entrySet()) {
            String host = resultEntry.getKey();
            Map<String, List<String>> consumers = resultEntry.getValue();
            for (Map.Entry<String, List<String>> entry : consumers.entrySet()) {
              String topic = entry.getKey();
              if (!root.containsKey(entry.getKey())) {
                root.put(topic, new ConcurrentHashMap<String, List<String>>());
              }
              Map<String, List<String>> m = root.get(topic);

              //add consumer:host to map
              for (String consumer : entry.getValue()) {
                if (!m.containsKey(consumer)) {
                  m.put(consumer, new ArrayList<String>());
                }
                List<String> h = m.get(consumer);
                h.add(host);
              }
            }
          }

          //build tree
          for (Map.Entry<String, Map<String, List<String>>> entry : root.entrySet()) {
            String topic = entry.getKey();
            StatusNode node = new StatusNode();
            node.id = topic;
            node.leaf = false;
            node.cls = StatusNode.CLS_FOLDER;
            node.text = topic;

            List<StatusNode> consumerNodeList = new ArrayList<StatusNode>();
            for (Map.Entry<String, List<String>> listEntry : entry.getValue().entrySet()) {
              StatusNode consumerNode = new StatusNode();
              String consumerId = listEntry.getKey();
              List<String> hostStrings = listEntry.getValue();

              consumerNode.id = consumerId + "@" + topic;
              consumerNode.cls = StatusNode.CLS_FOLDER;
              consumerNode.leaf = false;
              consumerNode.text = consumerId;

              List<StatusNode> hostNodeList = new ArrayList<StatusNode>();
              for (String host : hostStrings) {
                StatusNode hostNode = new StatusNode();

                hostNode.id = consumerId + "@" + topic + "@" + host;
                hostNode.cls = StatusNode.CLS_LEAF;
                hostNode.leaf = true;
                hostNode.text = host;

                hostNodeList.add(hostNode);

              }

              consumerNode.children = hostNodeList;

              consumerNodeList.add(consumerNode);
            }
            node.children = consumerNodeList;
            tree.add(node);
          }
        } else {

        }
      }

    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return JSON.toJSONString(tree);
  }

  @Override
  public String getConsumerStatus(String nodeId) throws Exception {
    if (nodeId == null || nodeId.isEmpty() || !nodeId.contains("@")) {
      return "nodeId=" + nodeId;
    }
    String[] nodeIdSplits = StringUtils.split(nodeId, "@");
    if (nodeIdSplits.length != 3 && nodeIdSplits.length != 4 && nodeIdSplits.length != 5) {
      return "nodeId=" + nodeId;
    }
    String consumerId = "";
    String topic = "";
    String host = "";
    String zone = null;
    String msBeanName = "";
    if (nodeIdSplits.length == 3) {
      consumerId = nodeIdSplits[0];
      topic = nodeIdSplits[1];
      host = nodeIdSplits[2];
    } else if (nodeIdSplits.length == 4) {
      consumerId = nodeIdSplits[0];
      topic = nodeIdSplits[1];
      host = nodeIdSplits[2];
    } else {
      consumerId = nodeIdSplits[0];
      topic = nodeIdSplits[1] + "@" + nodeIdSplits[2] + "@" + nodeIdSplits[3];
      host = nodeIdSplits[4];
    }

    if (consumerId.contains("#")) {
      String[] arr = consumerId.split("#");
      consumerId = arr[0];
      zone = arr[1];
    }

    msBeanName = (zone == null ? ("Consumer-" + topic + "-" + consumerId)
        : ("Consumer-" + topic + "-" + consumerId + "-" + zone));

    KuroroJmxService service = KuroroJmxService.getKuroroJmxClientInstance(host, PORT);
    Map<String, Object> map = service.getObjectAsMap(null, msBeanName);

    List<DefaultKeyValue> ret = new ArrayList<DefaultKeyValue>();
    ret.add(new DefaultKeyValue("TopicName", topic));
    ret.add(new DefaultKeyValue("ConsumerId", consumerId));
    Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Object> entry = it.next();
      String key = entry.getKey();
      String value = entry.getValue().toString();
      if ("ConsumerInfo".equals(key)) {
        String[] pairs = StringUtils.split(value, ",");
        for (String pair : pairs) {
          String[] kv = StringUtils.split(pair, "=");
          if ("ConsumerId".equals(kv[0])) {
            continue;
          }
          ret.add(new DefaultKeyValue(kv[0], kv[1]));
        }
        continue;
      }

      if ("FreeChannels".equals(key) || "ConnectedChannels".equals(key)) {
        value = StringUtils.replace(value, "/", "");
        value = StringUtils.replace(value, "isConnected:", "");
        value = StringUtils.replace(value, ")", "),");
        value = "[" + StringUtils.countMatches(value, ":") + "]:" + value;
      }

      if ("MessageFilter".equals(key)) {
        value = StringUtils.replace(value, "MessageFilter ", "");
      }

      if ("TopicName".equals(key) || "ConsumerId".equals(key)) {
        continue;
      }

      //end format
      ret.add(new DefaultKeyValue(key, value));
    }
    return JSON.toJSONString(ret);
  }

  @Override
  public String getCompensatorStatus(String nodeid) {
    String[] splits = StringUtils.split(nodeid, "@");
    if (splits.length != 3) {
      return "nodeid=" + nodeid;
    }
    List<DefaultKeyValue> result = new ArrayList<DefaultKeyValue>();
    String until = invokeJmxMethod(nodeid + "@getUntilOf");
    try {
      Long ts = Long.parseLong(StringUtils.split(until, ":")[0]);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(ts * 1000);

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

      result.add(new DefaultKeyValue("Until", until + " -> " + sdf.format(cal.getTime())));
    } catch (Exception e) {
      e.printStackTrace();
      result.add(new DefaultKeyValue("Until", until));
    }

    String list = invokeJmxMethod(nodeid + "@getCompensatingList");
    JSONArray jsonArray = (JSONArray) JSON.parse(list);
    int count = 0;

    for (Object ja : jsonArray) {
      JSONArray j = (JSONArray) ja;
      count += j.size();
    }
    result.add(new DefaultKeyValue("Compensation", count + " -> " + list));
    result.add(new DefaultKeyValue("Stop", nodeid + "@stopCompensation"));
    result.add(new DefaultKeyValue("Restart", nodeid + "@restartCompensation"));

    return JSON.toJSONString(result);
  }

  @Override
  public String invokeJmxMethod(String nodeid) {
    String[] splits = StringUtils.split(nodeid, "@");
    if (splits.length != 4) {
      return "nodeid=" + nodeid;
    }
    String consumerId = splits[0];
    String topic = splits[1];
    String host = splits[2];
    String method = splits[3];
    String zone = null;

    if (topic.contains("#")) {
      String[] arr = topic.split("#");
      topic = arr[0];
      zone = arr[1];
    }

    KuroroJmxService service = KuroroJmxService.getKuroroJmxClientInstance(host, PORT);
    try {
      return service.invokeMethod(method, topic, consumerId, zone);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  @Override
  public String listCompensatorTree(String group) {
    List<StatusNode> tree = new ArrayList<StatusNode>();
    try {
      Map<String, List<String>> hostsMap = getConsumerHosts();
      List<String> hosts = new ArrayList<String>();
      for (List<String> ls : hostsMap.values()) {
        if (ls.size() > 0) {
          hosts.addAll(ls);
        }
      }
      final Map<String, KuroroJmxService> serviceMap = new HashMap<String, KuroroJmxService>();

      for (String host : hosts) {
        serviceMap.put(host, KuroroJmxService.getKuroroJmxClientInstance(host, PORT));
      }

      if ("topic".equals(group)) {
        //topic,host,consumer
        Map<String, Map<String, List<String>>> compensators = new HashMap<String, Map<String, List<String>>>();

        for (String host : hosts) {
          KuroroJmxService service = serviceMap.get(host);
          boolean started = service.isCompensatorStarted();

          String str = service.getCompensating();
          String[] pairs = StringUtils.split(str, ",");
          for (String pair : pairs) {
            if (pair.contains("@")) {
              String[] kv = StringUtils.split(pair, "@");
              String topic = kv[1];
              String consumerId = kv[0];
              String zone = kv.length > 2 ? kv[2] : null;

              if (!KuroroUtil.isBlankString(zone)) {
                topic = topic + "#" + zone;
              }

              if (!compensators.containsKey(topic)) {
                compensators.put(topic, new HashMap<String, List<String>>());
              }
              Map<String, List<String>> map = compensators.get(topic);
              if (!map.containsKey(host)) {
                map.put(host, new ArrayList<String>());
              }
              List<String> list = map.get(host);
              if (!list.contains(consumerId)) {
                list.add(consumerId);
              }
            }
          }
        }

        for (Map.Entry<String, Map<String, List<String>>> entry : compensators.entrySet()) {
          String topic = entry.getKey();
          Map<String, List<String>> hostAndConsumerIds = entry.getValue();
          StatusNode topicNode = new StatusNode();

          topicNode.cls = StatusNode.CLS_FOLDER;
          topicNode.id = topic;
          topicNode.leaf = false;
          topicNode.text = topic;

          List<StatusNode> children = new ArrayList<StatusNode>();

          for (Map.Entry<String, List<String>> listEntry : hostAndConsumerIds.entrySet()) {
            String host = listEntry.getKey();
            List<String> consumerIds = listEntry.getValue();

            StatusNode hostNode = new StatusNode();

            hostNode.cls = StatusNode.CLS_FOLDER;
            hostNode.id = topic + "@" + host;
            hostNode.leaf = false;
            hostNode.text = host;

            List<StatusNode> conChildren = new ArrayList<StatusNode>();

            for (String consumerId : consumerIds) {
              StatusNode leafNode = new StatusNode();

              leafNode.cls = StatusNode.CLS_LEAF;
              leafNode.id = consumerId + "@" + topic + "@" + host;
              leafNode.leaf = true;
              leafNode.text = consumerId;

              conChildren.add(leafNode);
            }

            hostNode.children = conChildren;
            children.add(hostNode);
          }

          topicNode.children = children;
          tree.add(topicNode);
        }
      } else if ("leader".equals(group)) {
        //host,topic,consumer
        Map<String, Map<String, List<String>>> compensators = new HashMap<String, Map<String, List<String>>>();

        for (String host : hosts) {
          KuroroJmxService service = serviceMap.get(host);
          boolean started = service.isCompensatorStarted();
          if (started) {
            compensators.put(host, new HashMap<String, List<String>>());
          } else {
            compensators.put("[" + host + "]", new HashMap<String, List<String>>());
            continue;
          }
          Map<String, List<String>> map = compensators.get(host);

          String str = service.getCompensating();
          String[] pairs = StringUtils.split(str, ",");
          for (String pair : pairs) {
            if (pair.contains("@")) {
              String[] kv = StringUtils.split(pair, "@");
              String topic = kv[1];
              String consumerId = kv[0];

              String zone = kv.length > 2 ? kv[2] : null;

              if (!KuroroUtil.isBlankString(zone)) {
                topic = topic + "#" + zone;
              }

              if (!map.containsKey(topic)) {
                map.put(topic, new ArrayList<String>());
              }
              List<String> list = map.get(topic);
              if (!list.contains(consumerId)) {
                list.add(consumerId);
              }
            }
          }
        }
        for (Map.Entry<String, Map<String, List<String>>> entry : compensators.entrySet()) {
          String host = entry.getKey();
          Map<String, List<String>> topicAndConsumerIds = entry.getValue();
          StatusNode hostNode = new StatusNode();

          hostNode.cls = StatusNode.CLS_FOLDER;
          hostNode.id = host;
          hostNode.leaf = false;
          hostNode.text = host;

          List<StatusNode> children = new ArrayList<StatusNode>();

          for (Map.Entry<String, List<String>> listEntry : topicAndConsumerIds.entrySet()) {
            String topic = listEntry.getKey();
            List<String> consumerIds = listEntry.getValue();

            StatusNode topicNode = new StatusNode();

            topicNode.cls = StatusNode.CLS_FOLDER;
            topicNode.id = topic + "@" + host;
            topicNode.leaf = false;
            topicNode.text = topic;

            List<StatusNode> conChildren = new ArrayList<StatusNode>();

            for (String consumerId : consumerIds) {
              StatusNode leafNode = new StatusNode();

              leafNode.cls = StatusNode.CLS_LEAF;
              leafNode.id = consumerId + "@" + topic + "@" + host;
              leafNode.leaf = true;
              leafNode.text = consumerId;

              conChildren.add(leafNode);
            }

            topicNode.children = conChildren;
            children.add(topicNode);
          }

          hostNode.children = children;
          tree.add(hostNode);
        }
      }

      return JSON.toJSONString(tree);

    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  @Override
  public String getSyncTopicDestination() {
    List<DefaultKeyValue> result = new ArrayList<DefaultKeyValue>();
    try {
      Set<String> idcNameSet = MonitorZkUtil.getAllIdcName();
      for (String idcName : idcNameSet) {
        result.add(new DefaultKeyValue(idcName, idcName));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return JSON.toJSONString(result);
  }

  @Override
  public String getSyncZoneTopicDestination() {
    List<DefaultKeyValue> result = new ArrayList<DefaultKeyValue>();
    try {
      Set<String> zonesNameSet = KuroroZkUtil.localIdcZones();
      for (String zoneName : zonesNameSet) {
        result.add(new DefaultKeyValue(zoneName, zoneName));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return JSON.toJSONString(result);
  }

  @Override
  public String deleteTransferData(String topicRegionData) {

    try {
      JsonNode topicTransData = mapper.readTree(topicRegionData);
      String zone = topicTransData.get("destZone").asText();
      String idc = topicTransData.get("destIdc").asText();
      String dest = idc;
      if (KuroroUtil.isBlankString(idc)) {
        dest = zone;
      }
      StringBuilder topicTransPath = new StringBuilder(MonitorZkUtil.IDCZkTopicPath);
      topicTransPath.append(Constants.SEPARATOR).append(topicTransData.get("topic").asText());
      topicTransPath.append(Constants.SEPARATOR).append(dest);
      topicTransPath.append("#").append(topicTransData.get("consumerId").asText());
      topicTransPath.append("#").append(topicTransData.get("destTopic").asText());
      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      if (zkClient.exists(topicTransPath.toString())) {
        zkClient.delete(topicTransPath.toString());
        return MSG_OK;
      }
      return "not found transfer node for topic:" + topicTransData.get("topicName").asText();
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return "please retry after a moment";
  }

  @Override
  public String syncIdcTopics(String json) {
    String src = null, dest = null;
    String topicsMeta = null;
    boolean override;
    try {
      JsonNode data = mapper.readTree(json);
      src = data.get("src").asText();
      dest = data.get("dest").asText();
      override = data.get("override").asBoolean();

      if (src == null || src.equals(dest)) {
        return "can not Sync to a same IDC !";
      }

      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      if (zkClient.exists(KURORO_TOPIC_PATH)) {
        List<Object> objList = new ArrayList<Object>();
        List<String> childList = zkClient.getChildren(KURORO_TOPIC_PATH);
        if (childList == null) {
          return "can not find topics!";
        }
        for (String child : childList) {
          String childPath = KuroroUtil.getChildFullPath(KURORO_TOPIC_PATH, child);
          if (zkClient.exists(childPath)) {
            TopicConfigDataMeta meta = zkClient.readData(childPath);

            if (meta != null) {
              objList.add(meta);
              if (objList.size() >= 50) {
                topicsMeta = mapper.writeValueAsString(objList);

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("topicName", topicsMeta);
                jsonObject.put("override", override);

                AjaxRequest ajaxRequest = new AjaxRequest();
                ajaxRequest.setS("kuroroService");
                ajaxRequest.setM("writeTopicToZk");
                ajaxRequest.setI(dest);
                ajaxRequest.setP(jsonObject.toJSONString());

                String value = (String) requestIdcData(ajaxRequest, dest);
                if (!value.equals(MSG_OK)) {
                  for (int i = 0; i < retry; i++) {
                    if (value.equals(MSG_FAIL)) {
                      value = (String) requestIdcData(ajaxRequest, dest);
                    }
                    if (value.equals(MSG_OK)) {
                      break;
                    }
                  }
                }
                objList.clear();
              }
            }
          }
        }

      }
    } catch (Exception e) {
      return new StringBuilder("errorMsg").append(":").append(e.getMessage()).toString();
    }

    return MSG_OK;
  }

  @Override
  public String writeTopicToZk(String topicData) {
    try {
      ZkClient localZk = MonitorZkUtil.getLocalIDCZk();
      JSONObject jsonObject = JSON.parseObject(topicData);
      String topics = jsonObject.getString("topicName");
      boolean override = jsonObject.getBooleanValue("override");
      String mongoPath = MonitorZkUtil.IDCZkMongoPath;
      if (!localZk.exists(mongoPath)) {
        return "No mongo config in dest zone";
      }

      List<String> mongos = localZk.getChildren(mongoPath);
      Map<String, String> mongoMap = new HashMap<String, String>();
      for (String mongo : mongos) {
        String addr = localZk.readData(MonitorZkUtil.IDCZkMongoPath + Constants.SEPARATOR + mongo);
        mongoMap.put(mongo, addr);
      }

      List<Object> lists = JSONObject.parseObject(topics, List.class);

      for (Object o : lists) {
        TopicConfigDataMeta meta = JSON
            .parseObject(mapper.writeValueAsBytes(o), TopicConfigDataMeta.class);

        String topic = meta.getTopicName();
        String path = MonitorZkUtil.IDCZkTopicPath + Constants.SEPARATOR + topic;

        if (meta != null) {
          String mongoGroup = meta.getMongoGroup();

          List<String> mongoIpList = new ArrayList<String>();
          if (mongoGroup != null && !mongoGroup.isEmpty() && mongoMap.containsKey(mongoGroup)) {
            mongoIpList.add(mongoMap.get(mongoGroup));
            meta.setMongoGroup(mongoGroup);
          } else {
            for (String mong : mongoMap.keySet()) {
              mongoIpList.add(mongoMap.get(mong));
              meta.setMongoGroup(mong);
              break;
            }
          }
          meta.setReplicationSetList(mongoIpList);
        }

        if (!localZk.exists(path)) {
          localZk.createPersistent(path, true);
          localZk.writeData(path, meta);
        } else if (override) {
          localZk.writeData(path, meta);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("sync idc topics is errror!----" + topicData);
      return MSG_FAIL;

    }

    return MSG_OK;
  }

  //topics and transfer topics
  @Override
  public String syncZoneTopics(String json) {

    String src = null, dest = null;
    boolean override;
    try {
      JsonNode data = mapper.readTree(json);
      src = KuroroZkUtil.localZone();
      dest = data.get("dest").asText();
      override = data.get("override").asBoolean();

      if (src == null || dest == null) {
        return "can not Sync to a same zone or src can not null !";
      }

      if (KuroroZkUtil.localIdcAllZones().size() > 1 && dest.equals(src)) {
        return "when have 2 zk, can not sync idc topic to  idc zone  zk !";
      }

      ZkClient localZk = MonitorZkUtil.getLocalIDCZk();
      List<String> topics = localZk.getChildren(MonitorZkUtil.IDCZkTopicPath);
      ZkClient destZk = MonitorZkUtil.getIDCZK(dest);
      if (destZk == null) {
        return dest + " is not exist! sync topic failed";
      }
      for (String topic : topics) {
        String parentPath = KURORO_TOPIC_PATH + Constants.SEPARATOR + topic;
        String path =
            MonitorZkUtil.IDCZoneZKPath + Constants.SEPARATOR + dest + Constants.ZONE_TOPIC
                + Constants.SEPARATOR + topic;

        TopicConfigDataMeta meta = localZk.readData(parentPath);
        if (!destZk.exists(path)) {
          destZk.createPersistent(path, true);
          destZk.writeData(path, meta);
        } else if (override) {
          destZk.writeData(path, meta);
        }

        //transfer meta
        List<String> transTopic = localZk.getChildren(parentPath);
        for (String trans : transTopic) {
          String transParentPath = parentPath + Constants.SEPARATOR + trans;
          String transPath = path + Constants.SEPARATOR + trans;

          Object transMeta = localZk.readData(transParentPath);
          if (!destZk.exists(transPath)) {
            destZk.createPersistent(transPath, true);
            destZk.writeData(transPath, transMeta);
          } else if (override) {
            destZk.writeData(transPath, transMeta);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return MSG_FAIL + "===" + e.getMessage();
    }

    return MSG_OK;
  }

  @Override
  public String updateTransferDataStatus(String data) {
    try {
      JsonNode topicTransData = mapper.readTree(data);
      String zone = topicTransData.get("destZone").asText();
      String idc = topicTransData.get("destIdc").asText();
      String dest = idc;
      if (KuroroUtil.isBlankString(dest)) {
        dest = zone;
      }
      StringBuilder topicTransPath = new StringBuilder(MonitorZkUtil.IDCZkTopicPath);
      topicTransPath.append(Constants.SEPARATOR).append(topicTransData.get("topic").asText());
      topicTransPath.append(Constants.SEPARATOR).append(dest);
      topicTransPath.append("#").append(topicTransData.get("consumerId").asText());
      topicTransPath.append("#").append(topicTransData.get("destTopic").asText());
      boolean active = topicTransData.get("activeFlag").asBoolean();

      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      if (zkClient.exists(topicTransPath.toString())) {
        TopicZoneReplication tzr = zkClient.readData(topicTransPath.toString());
        tzr.setActiveFlag(active);
        zkClient.writeData(topicTransPath.toString(), tzr);
        return MSG_OK;
      }
      return "not found transfer node for topic:" + topicTransData.get("topicName").asText();
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return "please retry after a moment!";
  }

  @Override
  public String modifyTransferDataStatus(String data) {
    try {
      JsonNode topicTransData = mapper.readTree(data);
      String zone = topicTransData.get("destZone").asText();
      String idc = topicTransData.get("destIdc").asText();
      String dest = idc;
      if (KuroroUtil.isBlankString(idc)) {
        dest = zone;
      }
      StringBuilder path = new StringBuilder(MonitorZkUtil.IDCZkTopicPath);
      path.append(Constants.SEPARATOR).append(topicTransData.get("topic").asText());
      path.append(Constants.SEPARATOR).append(dest);
      path.append("#").append(topicTransData.get("consumerId").asText());
      path.append("#").append(topicTransData.get("destTopic").asText());
      String topicTransPath = path.toString();
      int speed = topicTransData.get("speed").asInt();
      String group = topicTransData.get("group").asText();
      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();

      if (zkClient.exists(topicTransPath)) {
        TopicZoneReplication tzr = zkClient.readData(topicTransPath);
        if (tzr.getConsumerSpeed() == speed && tzr.getGroup().equalsIgnoreCase(group)) {
          return MSG_OK;
        }

        zkClient.delete(topicTransPath);
        Thread.currentThread().sleep(2000);
        tzr.setConsumerSpeed(speed);
        tzr.setGroup(group);
        zkClient.createPersistent(topicTransPath, tzr);

        return MSG_OK;
      }
      return "not found transfer node for topic:" + topicTransData.get("topicName").asText();
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return "please retry after a moment";
  }

  /**
   * 获取所有消费服务器的host
   */
  private Map<String, List<String>> getConsumerHosts() throws Exception {
    ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
    List<String> groups = null;
    if (zkClient.exists(MonitorZkUtil.IDCZkBrokerGroupPath)) {
      groups = zkClient.getChildren(MonitorZkUtil.IDCZkBrokerGroupPath);
      if (groups == null) {
        return null;
      }
    }

    HashMap<String, List<String>> hostsToGroup = new HashMap<String, List<String>>();
    for (String name : groups) {
      List<String> tempHosts = zkClient.getChildren(
          MonitorZkUtil.IDCZkBrokerGroupPath + Constants.SEPARATOR + name + "/Consumer");
      hostsToGroup.put(name, tempHosts);
    }

    return hostsToGroup;
  }

  @Override
  public String getMongoGroupList(String idc) {
	  if (KuroroUtil.isBlankString(idc)) {
		  return null;
	  }

    ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
    List<String> mongoList = zkClient.getChildren(MonitorZkUtil.IDCZkMongoPath);
    List<HashMap<String, String>> mongoMap = new ArrayList<HashMap<String, String>>();

    for (String mongos : mongoList) {
      HashMap<String, String> map = new HashMap<String, String>();
      String addr = zkClient.readData(MonitorZkUtil.IDCZkMongoPath + Constants.SEPARATOR + mongos);
      map.put("text", mongos);
      map.put("value", addr);
      mongoMap.add(map);
    }
    ComparatorHashMap comparator = new ComparatorHashMap();
    Collections.sort(mongoMap, comparator);

    return JSON.toJSONString(mongoMap);
  }

  @Override
  public String batchEditTopicMongo(String jsonData) {
    ZkClient _zkClient = null;
    String result = null;

    JSONObject jsonObj = JSON.parseObject(jsonData);
    JSONArray topicsArray = jsonObj.getJSONArray("topics");
    String mongoURL = jsonObj.getString("mongoURL");
    String mongoGroup = jsonObj.getString("mongoGroup");

    List<String> mongoList = new ArrayList<String>();
    mongoList.add(mongoURL);

    if (topicsArray.size() < 1 || mongoURL == null || mongoGroup == null) {
      return "topic or mongoURL or zone is null";
    }
    try {
      _zkClient = MonitorZkUtil.getLocalIDCZk();
      for (int i = 0; i < topicsArray.size(); i++) {
        String childPath = KuroroUtil
            .getChildFullPath(KURORO_TOPIC_PATH, (String) topicsArray.get(i));
        if (_zkClient.exists(childPath)) {
          TopicConfigDataMeta data = _zkClient.readData(childPath);
          if (data != null) {
            if (data.getReplicationSetList().get(0).equals(mongoURL)) {
              continue;
            } else {
              data.setReplicationSetList(mongoList);
              data.setMongoGroup(mongoGroup);
              _zkClient.writeData(childPath, data);
            }
          }
        }
      }
      result = "change mongoURL of topics is  success";
    } catch (Exception e) {
      result = new StringBuilder("errorMsg").append(":").append(e.getMessage()).toString();
    }
    return result;
  }

  @Override
  public String mongoQuery(String jsonData) {

    String CollectName = "c";
    String table_producer = "msg_", table_consumer = "ack_";
    JSONObject jsonObj = JSON.parseObject(jsonData);
    String queryType = jsonObj.getString("queryType");
    String tableHead = jsonObj.getString("tableHead");
    int limitSize = jsonObj.getIntValue("querySize");
    String topicName = jsonObj.getString("topicName");
    String consumerId = jsonObj.getString("consumerId");
    String zoneName = jsonObj.getString("zoneName");
    boolean consumerLevel = jsonObj.getBoolean("consumerLevel");
    String queryResult = "";
    String DBName = null;
    DBCursor cursor = null;
    TopicConfigDataMeta topicMetaData = null;

    if (queryType == null || tableHead == null || topicName == null) {
      return "查询内容不能为空！...";
    }
    if (limitSize < 1) {
      limitSize = 1;
    }

    try {
      topicMetaData = mapper.readValue(getTopic(topicName), TopicConfigDataMeta.class);
      if (topicMetaData == null || topicMetaData.getReplicationSetList().get(0) == null) {
        return "topicData of read zk error.....";
      }
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    MongoClient mongoClient = getMongoClient(topicMetaData.getReplicationSetList().get(0));
    if (tableHead.equals(table_producer)) {
      DBName = tableHead + topicName;
    } else if (tableHead.equals(table_consumer) && consumerId != null) {
      if (!KuroroUtil.isBlankString(zoneName) && consumerLevel) {
        DBName = tableHead + topicName + "#" + consumerId + "#" + zoneName;
      } else {
        DBName = tableHead + topicName + "#" + consumerId;
      }
    } else {
      return "send args error....";
    }

    DBCollection collection = mongoClient.getDB(DBName).getCollection(CollectName);
    DBObject gt = null, query = null, orderBy = null;

    if (queryType.equals("timeStampQuery")) {
      Long timeStamp = jsonObj.getLong("messageId");
      if (timeStamp == null) {
        return "time 不能为空!...";
      }
      query = BasicDBObjectBuilder.start().add(ID, MongoUtil.longToBSONTimestamp(timeStamp)).get();
    } else if (queryType.equals("regexQuery")) {
      String queryField = jsonObj.getString("queryField");
      String queryContent = jsonObj.getString("queryContent");
      if (queryField == null || queryContent == null) {
        return "查询内容不能为空！...";
      }
      gt = BasicDBObjectBuilder.start().add("$regex", queryContent).get();
      query = BasicDBObjectBuilder.start().add(queryField, gt).get();
    }

    orderBy = BasicDBObjectBuilder.start().add(ID, Integer.valueOf(-1)).get();
    if (queryType.equals("timeStampQuery") || queryType.equals("regexQuery")) {
      cursor = collection.find(query).sort(orderBy).limit(limitSize);
    } else if (queryType.equals("newDataQuery")) {
      if (query == null) {
        cursor = collection.find(null, null).sort(orderBy).limit(limitSize);
      } else {
        cursor = collection.find(query).sort(orderBy).limit(limitSize);
      }
    } else if (queryType.equals("oldDataQuery")) {
      orderBy = BasicDBObjectBuilder.start().add(ID, Integer.valueOf(1)).get();
      if (query == null) {
        cursor = collection.find(null, null).sort(orderBy).limit(limitSize);
      } else {
        cursor = collection.find(query).sort(orderBy).limit(limitSize);
      }
    }

    try {
      int countNum = 1;
      if (cursor.hasNext()) {
        while (cursor.hasNext()) {
          DBObject result = cursor.next();
          if (tableHead.equals(table_consumer)) {
            queryResult += "第 " + countNum + " 条消息\n " + result + '\n' + '\n';
          } else if (tableHead.equals(table_producer)) {
            KuroroMessage kuroroMessage = new KuroroMessage();
            convert(result, kuroroMessage);
            queryResult +=
                "第 " + countNum + " 条消息\n " + kuroroMessage.toString() + '\n' + '\n' + result + '\n'
                    + '\n';
          } else {
            queryResult = "query mongo is error.....sorry...";
          }
          countNum++;
        }
      } else {
        queryResult = "没有相关的内容...";
      }
    } catch (Exception e) {
      if (e.getMessage().contains("Timeout while receiving message")) {
        queryResult = "没有相关的内容...";
      } else {
        queryResult = "mongodb查询出错,请稍后再试!";
        logger.error("查询超时，没有相关内容...", e);
      }
    } finally {
      cursor.close();
      mongoClient.close();
    }
    return queryResult;
  }

  @Override
  public Map<String, Object> getLocalIdcZones() {
    Set<String> zoneSet = KuroroZkUtil.localIdcZones();
    Map<String, Object> resultMap = parseListToUIMap(zoneSet);
    return resultMap;
  }

  @Override
  public Map<String, Object> getLocalZoneName() {
    String zoneName = KuroroZkUtil.localZone();
    Set<String> zoneSet = new HashSet<String>();
    zoneSet.add(zoneName);
    Map<String, Object> resultMap = parseListToUIMap(zoneSet);
    return resultMap;
  }

  @Override
  public Map<String, Object> getIDCAllZones() {
    Set<String> zones = KuroroZkUtil.allIdcZoneName();
    Map<String, Object> resultMap = parseListToUIMap(zones);
    return resultMap;
  }

  private Map<String, Object> parseListToUIMap(Collection dataList) {
    Map<String, Object> resultMap = new HashMap<String, Object>();
    if (dataList != null && dataList.size() > 0) {
      List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
      for (Object data : dataList) {
        Map<String, Object> item = new HashMap<String, Object>();
        item.put("value", data);
        item.put("text", data);
        list.add(item);
      }
      resultMap.put("rows", list);
    }
    return resultMap;
  }

  private MongoClient getMongoClient(String mongoURL) {
    List<ServerAddress> replicaSetSeeds = parseUriToAddressList(mongoURL);
    String mongoUserName = PropertiesContainer.getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, InternalPropKey.MONGO_PASSWORD);
    char[] pwd_char = PropertiesContainer.getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, InternalPropKey.MONGO_USERNAME).toCharArray();
    MongoCredential credent = MongoCredential.createCredential(mongoUserName, "admin", pwd_char);
    List<MongoCredential> credentList = new ArrayList<MongoCredential>();
    credentList.add(credent);
    MongoClient mongoClient = new MongoClient(replicaSetSeeds, credentList,
        getMongoClientOptions());
    mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
    return mongoClient;
  }

  private List<ServerAddress> parseUriToAddressList(String uri) {
    uri = uri.trim();
    String schema = "mongodb://";
    if (uri.startsWith(schema)) {
      uri = uri.substring(schema.length());
    }
    String hostPortArr[] = uri.split(",");
    List<ServerAddress> result = new ArrayList<ServerAddress>();
    for (String hostPort : hostPortArr) {
      String pair[] = hostPort.split(":");
      try {
        result.add(new ServerAddress(pair[0].trim(), Integer.parseInt(pair[1].trim())));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            (new StringBuilder()).append(e.getMessage()).append(".Bad format of mongo uri:")
                .append(uri)
                .append(".The correct format is mongodb://<host>:<port>,<host>:<port>").toString(),
            e);
      }
    }
    return result;
  }

  private void convert(DBObject result, KuroroMessage kuroroMessage) {
    BSONTimestamp timestamp = (BSONTimestamp) result.get(ID);
    kuroroMessage.setMessageId(MongoUtil.BSONTimestampToLong(timestamp));
    kuroroMessage.setCreatedTime((String) result.get(CREATETIME));// generatedTime
    kuroroMessage.setZone((String) result.get("ze"));
    kuroroMessage.setPoolId((String) result.get("pd"));
    Map<String, String> propertiesBasicDBObject = (Map<String, String>) result.get(PROPERTIES);
    if (propertiesBasicDBObject != null) {
      HashMap<String, String> properties = new HashMap<String, String>(propertiesBasicDBObject);
      kuroroMessage.setProperties(properties);// properties
    }
    Map<String, String> internalPropertiesBasicDBObject = (Map<String, String>) result
        .get(INTERNAL_PROPERTIES);
    if (internalPropertiesBasicDBObject != null) {
      HashMap<String, String> properties = new HashMap<String, String>(
          internalPropertiesBasicDBObject);
      kuroroMessage.setInternalProperties(properties);// properties
    }
    kuroroMessage.setType((String) result.get(TYPE));// type
    kuroroMessage.setSourceIp((String) result.get(SOURCE_IP));// sourceIp

    String protocolType = (String) result.get(PROTOCOLTYPE);
	  if (protocolType != null) {
		  kuroroMessage.setProtocolType(protocolType);// protocolType
	  }

    try {
      if (kuroroMessage.getInternalProperties() != null) {
        if ("gzip".equals(kuroroMessage.getInternalProperties().get("compress"))) {
          kuroroMessage.setContent(ZipUtil.unzip((String) result.get(CONTENT)));
        }
      } else {
        kuroroMessage.setContent(result.get(CONTENT));// content
      }

      if (protocolType.equals("HESSIAN")) {
        JSONObject jsonObject = JSON.parseObject(kuroroMessage.getContent());
        Object messageDecode = hessianMessageDecode(jsonObject.getString("data"),
            (Integer) jsonObject.get("flag"));
        if (messageDecode != null) {
          kuroroMessage.setMessageContent(messageDecode);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      kuroroMessage.setContent(result.get(CONTENT));// content
    }
  }

  private void consumerConvert(DBObject result, MessageView messageView) {
    BSONTimestamp timestamp = (BSONTimestamp) result.get(ID);
    messageView.setCTimeStamp(timestamp.getTime());
    messageView.setCInc(timestamp.getInc());
    messageView.setCConsumerIp((String) result.get("cip"));
    messageView.setcConsumerTime((String) result.get("ct"));
    messageView.setCZone((String) result.get("ze"));
    messageView.setCPoolId((String) result.get("pd"));
  }

  private MongoClientOptions getMongoClientOptions() {
    MongoClientOptions.Builder builder = new MongoClientOptions.Builder();

    builder.connectionsPerHost(30);
    builder.socketKeepAlive(true);
    builder.socketTimeout(5000);
    builder.threadsAllowedToBlockForConnectionMultiplier(10);
    builder.connectTimeout(5000);
    builder.maxWaitTime(5000);
    builder.serverSelectionTimeout(2000);
    MongoClientOptions options = builder.build();

    return options;
  }

  @Override
  public Map<String, Object> getAllZoneDic(String excludeZone) {
    Set<Zone> az = MonitorZkUtil.getAllZonesInCurrentIdc();
    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    if (az != null) {
      if (excludeZone != null && !"".equals(excludeZone)) {
        Iterator<Zone> it = az.iterator();
        while (it.hasNext()) {
          Zone z = it.next();
          if (z != null && excludeZone.equals(z.getName())) {
            it.remove();
          }
        }
      }
      for (Zone z : az) {
        Map<String, Object> temp = new HashMap<String, Object>();
        temp.put("value", z.getName());
        temp.put("text", z.getAlias());
        list.add(temp);
      }
      map.put("root", list);
    }
    return map;
  }

  @Override
  public Map<String, Object> getAllIdcDic(String excludeidc) {
    Set<String> idcs = MonitorZkUtil.getAllIdcName();
    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    if (idcs != null) {
      if (excludeidc != null && !"".equals(excludeidc)) {
        Iterator<String> it = idcs.iterator();
        while (it.hasNext()) {
          String idc = it.next();
          if (KuroroUtil.isBlankString(idc)) {
            it.remove();
            continue;
          }
          if (idc == null || excludeidc.equals(idc)) {
            it.remove();
          }
        }
      }
      for (String idc : idcs) {
        Map<String, Object> temp = new HashMap<String, Object>();
        temp.put("value", idc);
        temp.put("text", idc);
        list.add(temp);
      }
      map.put("root", list);
    }
    return map;
  }

  @Override
  public String getAllIdcName(String excludeidc) {
    Set<String> idcs = MonitorZkUtil.getAllIdcName();
    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    if (idcs != null) {
      if (excludeidc != null && !"".equals(excludeidc)) {
        Iterator<String> it = idcs.iterator();
        while (it.hasNext()) {
          String idc = it.next();
          if (KuroroUtil.isBlankString(idc)) {
            it.remove();
            continue;
          }

          if (idc == null || excludeidc.equals(idc)) {
            it.remove();
          }
        }
      }
      for (String idc : idcs) {
        Map<String, Object> temp = new HashMap<String, Object>();
        temp.put("value", idc);
        temp.put("text", idc);
        list.add(temp);
      }
    }
    return JSON.toJSONString(list);
  }

  @Override
  public String getLocalIdcName() {
    return MonitorZkUtil.localIDCName;
  }

  private Object requestIdcData(AjaxRequest ajaxRequest, String destIdc) {
    Object value = MSG_OK;
    try {
      String url = KuroroUtil.currentOpenAPI(destIdc);
      Map<String, String> params = new HashMap<String, String>();
      params.put("rmi", JSONObject.toJSONString(ajaxRequest));

      value = MonitorHttpClientUtil.remoteExcuter(url, params);

    } catch (Exception e) {
      e.printStackTrace();
      return MSG_FAIL;

    }
    return value;
  }

  public void updateRemoteProducerServerIpListToLocalIdcZk(String p) {
    String[] Parameters = p.split("@");
    String idcName = Parameters[0];
    String[] remoteIpArray = Parameters[1].split(",");
    List<String> remoteIps = Arrays.asList(remoteIpArray);
    ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
    String path = MonitorZkUtil.IDCZKRemoteProducerPath + Constants.SEPARATOR + idcName;
    List<String> localIps = null;
    if (zkClient.exists(path)) {
      localIps = zkClient.getChildren(path);
    }
    List<String> addList = new ArrayList<String>();
    List<String> delList = new ArrayList<String>();
    for (String remoteIp : remoteIps) {
      if (!localIps.contains(remoteIp)) {
        addList.add(remoteIp);
      }
    }
    for (String ip : addList) {
      HostInfo hostInfo = new HostInfo(ip, Constants.KURORO_PRODUCER_PORT, 1);
      if (!zkClient.exists(path)) {
        zkClient.createPersistent(path);
      }
      zkClient.createPersistent(path + Constants.SEPARATOR + ip, hostInfo);
    }

    for (String localIp : localIps) {
      if (!remoteIps.contains(localIp)) {
        delList.add(localIp);
      }
    }
    for (String ip : delList) {
      zkClient.delete(path + Constants.SEPARATOR + ip);
    }
  }

  public String getProducerServerIpList() {
    String returnString = "";
    ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
    String path = MonitorZkUtil.IDCZkProductServerDefaultPath;
    List<String> ipList = zkClient.getChildren(path);
    if (ipList != null && !ipList.isEmpty()) {
      for (int i = 0; i < ipList.size(); i++) {
        returnString = returnString + ipList.get(i);
        if (i < (ipList.size() - 1)) {
          returnString = returnString + ",";
        }
      }
    }
    return returnString;
  }

  @Override
  public String deleteTransferTopic(String transferTopicPath) {
    try {
      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      if (zkClient.exists(transferTopicPath)) {
        zkClient.delete(transferTopicPath);
        return "success";
      } else {
        return "NonExistent";
      }
    } catch (Exception e) {
      logger.error(
          "execute KuroroServiceImpl#deleteTransferTopic fail! parameters:" + transferTopicPath, e);
    }
    return "fail";
  }

  @Override
  public String createTransferTopic(String jsonData) {
    try {
      TopicConfigDataMeta topicData = mapper.readValue(jsonData, TopicConfigDataMeta.class);
      String topicName = topicData.getTopicName();
      StringBuilder path = new StringBuilder(MonitorZkUtil.IDCZkTopicPath);
      path.append(Constants.SEPARATOR);
      path.append(topicName);
      String transferTopicPath = path.toString();
      ZkClient zkClient = MonitorZkUtil.getLocalIDCZk();
      if (!zkClient.exists(transferTopicPath)) {
        zkClient.createPersistent(transferTopicPath, topicData);
        return "success";
      } else {
        return "NodeExists";
      }
    } catch (Exception e) {
      logger.error("execute KuroroServiceImpl#createTransferTopic fail! parameters:" + jsonData, e);
    }
    return "fail";
  }

  public Object hessianMessageDecode(String content, int messageFlag) {
    Object messageContent = null;
    Transcoder transcoder = HessianTranscoder.getTranscoder();
    JsonBinder jb = JsonBinder.getNonEmptyBinder();
    Map<String, Object> x = new HashMap<String, Object>();
    x.put("data", content);
    x.put("flag", messageFlag);
    String content2 = jb.toJson(x);
    Map<String, Object> c = jb.fromJson(content2, Map.class);
    byte[] v = jb.fromJson(c.get("data").toString(), byte[].class);
    CachedData cachedData = new CachedData(v, messageFlag);
    messageContent = transcoder.decode(cachedData);

    return messageContent;
  }

  @Override
  public String messageTrackQuery(String jsonData) {

    String CollectName = "c";
    JSONObject jsonObj = JSON.parseObject(jsonData);
    String queryType = jsonObj.getString("queryType");
    int limitSize = jsonObj.getIntValue("querySize");
    String topicName = jsonObj.getString("topicName");
    String DBName = null;
    DBCursor cursor = null;
    TopicConfigDataMeta topicMetaData = null;
    String resultValue = null;

    if (queryType == null || topicName == null) {
      return "查询内容不能为空！...";
    }
    if (limitSize < 1) {
      limitSize = 1;
    }

    try {
      topicMetaData = mapper.readValue(getTopic(topicName), TopicConfigDataMeta.class);
      if (topicMetaData == null || topicMetaData.getReplicationSetList().get(0) == null) {
        return "topicData of read zk error.....";
      }
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    MongoClient mongoClient = getMongoClient(topicMetaData.getReplicationSetList().get(0));
    DBName = "msg_" + topicName;
    DBCollection collection = mongoClient.getDB(DBName).getCollection(CollectName);
    DBObject gt = null, query = null, orderBy = null;
    orderBy = BasicDBObjectBuilder.start().add(ID, Integer.valueOf(-1)).get();
    List<Object> messageList = new ArrayList<Object>();

    if (queryType.equals("timeStampQuery")) {
      Long timeStamp = jsonObj.getLong("messageId");
      if (timeStamp == null) {
        return "time 不能为空!...";
      }
      query = BasicDBObjectBuilder.start().add(ID, MongoUtil.longToBSONTimestamp(timeStamp)).get();
    } else if (queryType.equals("regexQuery")) {
      String queryField = jsonObj.getString("queryField");
      String queryContent = jsonObj.getString("queryContent");
      if (queryField == null || queryContent == null) {
        return "查询内容不能为空！...";
      }
      gt = BasicDBObjectBuilder.start().add("$regex", queryContent).get();
      query = BasicDBObjectBuilder.start().add(queryField, gt).get();
    }

    if (queryType.equals("timeStampQuery") || queryType.equals("regexQuery")) {
      cursor = collection.find(query).sort(orderBy).limit(limitSize);
    } else if (queryType.equals("newDataQuery")) {
      if (query == null) {
        cursor = collection.find(null, null).sort(orderBy).limit(limitSize);
      } else {
        cursor = collection.find(query).sort(orderBy).limit(limitSize);
      }
    } else if (queryType.equals("oldDataQuery")) {
      orderBy = BasicDBObjectBuilder.start().add(ID, Integer.valueOf(1)).get();
      if (query == null) {
        cursor = collection.find(null, null).sort(orderBy).limit(limitSize);
      } else {
        cursor = collection.find(query).sort(orderBy).limit(limitSize);
      }
    }

    try {
      int countNum = 1;
      if (cursor.hasNext()) {
        while (cursor.hasNext()) {
          DBObject result = cursor.next();

          KuroroMessage kuroroMessage = new KuroroMessage();
          convert(result, kuroroMessage);
          BSONTimestamp timestamp = (BSONTimestamp) result.get(ID);
          MessageView messageView = new MessageView();
          messageView.setPTimeStamp(timestamp.getTime());
          messageView.setPInc(timestamp.getInc());
          messageView.setPContent(kuroroMessage.getContent());
          messageView.setPSourceIp(kuroroMessage.getSourceIp());
          messageView.setPCreatedTime(kuroroMessage.getCreatedTime());
          messageView.setPType(kuroroMessage.getType());
          messageView.setPCompression(
              kuroroMessage.getInternalProperties() != null ? kuroroMessage.getInternalProperties()
                  .get("compress") : "否");
          messageView.setPProtocolType(kuroroMessage.getProtocolType());
          messageView.setPZone(kuroroMessage.getZone());
          messageView.setPPoolId(kuroroMessage.getPoolId());
          if (kuroroMessage.getMessageContent() != null) {
            messageView.setPObectContent(kuroroMessage.getMessageContent().toString());
          }
          HashMap<String, Object> map = JSON
              .parseObject(mapper.writeValueAsBytes(messageView), HashMap.class);
          messageList.add(map);
        }
        countNum++;
      }

    } catch (Exception e) {
      if (e.getMessage().contains("Timeout while receiving message")) {
      } else {
        logger.error("查询超时，没有相关内容...", e);
      }
    } finally {
      cursor.close();
      mongoClient.close();
    }
    try {
      resultValue = mapper.writeValueAsString(messageList);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return resultValue;
  }

  @Override
  public List<MessageView> queryMessageDetail(String json) {
    JSONObject jsonObj = JSON.parseObject(json);
    int timestamp = jsonObj.getIntValue("timestamp");
    int inc = jsonObj.getIntValue("inc");
    String topicName = jsonObj.getString("topicName");
    String consumerId = jsonObj.getString("consumerId");
    String zone = jsonObj.getString("zone");
    TopicConfigDataMeta topicMetaData = null;
    DBCursor cursor = null;
    BSONTimestamp bsonTimestamp = new BSONTimestamp(timestamp, inc);
    List<MessageView> messageViewList = new ArrayList<MessageView>();

    try {
      topicMetaData = mapper.readValue(getTopic(topicName), TopicConfigDataMeta.class);
      if (topicMetaData == null || topicMetaData.getReplicationSetList().get(0) == null) {
        throw new NullPointerException("can not find topic " + topicName + " from zk !");
      }
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    MongoClient mongoClient = getMongoClient(topicMetaData.getReplicationSetList().get(0));
    DBObject orderBy = BasicDBObjectBuilder.start().add(ID, Integer.valueOf(-1)).get();
    MessageView messageView = new MessageView();

    //query topic producer message
    try {
      String producerName = "msg_" + topicName;
      DBCollection producerColl = mongoClient.getDB(producerName).getCollection("c");
      DBObject query = BasicDBObjectBuilder.start().add(ID, bsonTimestamp).get();
      cursor = producerColl.find(query).sort(orderBy).limit(1);
      while (cursor.hasNext()) {
        DBObject value = cursor.next();
        KuroroMessage kuroroMessage = new KuroroMessage();
        convert(value, kuroroMessage);

        messageView.setPTimeStamp(bsonTimestamp.getTime());
        messageView.setPInc(bsonTimestamp.getInc());
        messageView.setPContent(kuroroMessage.getContent());
        messageView.setPSourceIp(kuroroMessage.getSourceIp());
        messageView.setPCreatedTime(kuroroMessage.getCreatedTime());
        messageView.setPType(kuroroMessage.getType());
        messageView.setPCompression(
            kuroroMessage.getInternalProperties() != null ? kuroroMessage.getInternalProperties()
                .get("compress") : "否");
        messageView.setPProtocolType(kuroroMessage.getProtocolType());
        messageView.setPZone(kuroroMessage.getZone());
        messageView.setPPoolId(kuroroMessage.getPoolId());
		  if (kuroroMessage.getMessageContent() != null) {
			  messageView.setPObectContent(kuroroMessage.getMessageContent().toString());
		  }
      }
      cursor.close();
    } catch (Exception e) {

    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }

    //query consumer ack
    try {
      if (!KuroroUtil.isBlankString(consumerId)) {
        String consumerName = "ack_" + topicName + "#" + consumerId;
        if (!KuroroUtil.isBlankString(zone)) {
          consumerName = consumerName + "#" + zone;
        }
        DBCollection consumerColl = mongoClient.getDB(consumerName).getCollection("c");
        DBObject query = BasicDBObjectBuilder.start().add(ID, bsonTimestamp).get();
        cursor = consumerColl.find(query).sort(orderBy);
        while (cursor.hasNext()) {
          MessageView consumerView = (MessageView) messageView.clone();
          DBObject value = cursor.next();
          consumerConvert(value, consumerView);
          messageViewList.add(consumerView);
        }
        cursor.close();
        mongoClient.close();
      } else {
        messageViewList.add(messageView);
      }
    } catch (Exception e) {

    } finally {
      if (cursor != null) {
        cursor.close();
      }
      if (mongoClient != null) {
        mongoClient.close();
      }
    }

    return messageViewList;
  }

  private static class StatusNode {

    public static final String CLS_FOLDER = "folder";
    public static final String CLS_LEAF = "file";
    public static List<StatusNode> EMPTY = new ArrayList<StatusNode>();
    private String cls;
    private String id;
    private Boolean leaf;
    private String text;
    private List<StatusNode> children = EMPTY;

    public String getCls() {
      return cls;
    }

    public void setCls(String cls) {
      this.cls = cls;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public Boolean getLeaf() {
      return leaf;
    }

    public void setLeaf(Boolean leaf) {
      this.leaf = leaf;
    }

    public String getText() {
      return text;
    }

    public void setText(String text) {
      this.text = text;
    }

    public List<StatusNode> getChildren() {
      return children;
    }

    public void setChildren(List<StatusNode> children) {
      this.children = children;
    }
  }
}

/*
 * Positive sort list 
 * */
class ComparatorHashMap implements Comparator<HashMap<String, String>> {

  public int compare(HashMap<String, String> map1, HashMap<String, String> map2) {

    int flag = (map1.get("text").toString()).compareTo(map2.get("text").toString());

    return flag;

  }
}

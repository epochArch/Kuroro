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

package com.epocharch.kuroro.common.inner.dao.impl.mongodb;

import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;
import com.mongodb.WriteConcern;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDAOImpl implements MessageDAO {

  private static final Logger LOG = LoggerFactory.getLogger(MessageDAOImpl.class);
  public static final String OID = "oid";
  public static final String CONTENT = "c";
  public static final String VERSION = "v";
  public static final String SHA1 = "s";
  public static final String GENERATED_TIME = "gt";
  public static final String PROPERTIES = "p";
  public static final String INTERNAL_PROPERTIES = "_p";
  public static final String TYPE = "t";
  public static final String SOURCE_IP = "si";
  public static final String PROTOCOLTYPE = "pt";
  public static final String UUID = "uid";
  public static final String CREATED_TIME = "ct";
  public static final String NID = "nid";
  public static final String IDC = "idc";
  public static final String ZONE = "ze";
  public static final String POOLID = "pd";
  private NewMongoClient newMongoClient;

  public MessageDAOImpl() {

  }

  public void setNewMongoClient(NewMongoClient newMongoClient) {
    this.newMongoClient = newMongoClient;
  }


  @Override
  public KuroroMessage getMessage(String topicName, Long messageId) {
    return getMessage(topicName, messageId, 0);
  }

  @Override
  public KuroroMessage getMessage(String topicName, Long messageId, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject query = BasicDBObjectBuilder.start().add(NID, MongoUtil.longToBSONTimestamp(messageId))
        .get();
    DBObject result = collection.findOne(query);
    if (result != null) {
      KuroroMessage kuroroMessage = new KuroroMessage();
      try {
        convert(result, kuroroMessage);
        return kuroroMessage;
      } catch (RuntimeException e) {
        LOG.error("Error when convert resultset to kuroroMessage.", e);
      }
    }
    return null;
  }

  public DBObject getMessageByUUId(String topicName, ObjectId uuid, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject query = BasicDBObjectBuilder.start().add(UUID, uuid).get();
    DBObject result = collection.findOne(query);
    return result;
  }

  @Override
  public Long getMaxMessageId(String topicName) {
    return getMaxMessageId(topicName, 0);
  }

  @Override
  public Long getMaxMessageId(String topicName, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject fields = BasicDBObjectBuilder.start().add(NID, 1).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(-1)).get();
    DBCursor cursor = collection.find(null, fields).sort(orderBy).limit(1);
    try {
      if (cursor.hasNext()) {
        BSONTimestamp timestamp = (BSONTimestamp) cursor.next().get(NID);
        return MongoUtil.BSONTimestampToLong(timestamp);
      }
    } finally {
      cursor.close();
    }
    return null;
  }

  @Override
  public Long getMinMessageId(String topicName, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject fields = BasicDBObjectBuilder.start().add(NID, 1).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBCursor cursor = collection.find(null, fields).sort(orderBy).limit(1);
    try {
      if (cursor.hasNext()) {
        BSONTimestamp timestamp = (BSONTimestamp) cursor.next().get(NID);
        return MongoUtil.BSONTimestampToLong(timestamp);
      }
    } finally {
      cursor.close();
    }
    return null;
  }

  @Override
  public KuroroMessage getMaxMessage(String topicName) {
    return getMaxMessage(topicName, 0);
  }

  @Override
  public KuroroMessage getMaxMessage(String topicName, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(-1)).get();
    DBCursor cursor = collection.find().sort(orderBy).limit(1);
    try {
      if (cursor.hasNext()) {
        DBObject result = cursor.next();
        KuroroMessage kuroroMessage = new KuroroMessage();
        try {
          convert(result, kuroroMessage);
          return kuroroMessage;
        } catch (RuntimeException e) {
          LOG.error("Error when convert resultset to kuroroMessage.", e);
        }
      }
    } finally {
      cursor.close();
    }
    return null;
  }

  @Override
  public List<KuroroMessage> getMessagesGreaterThan(String topicName, Long messageId, int size) {
    return getMessagesGreaterThan(topicName, messageId, size, 0);
  }

  @Override
  public List<KuroroMessage> getMessagesGreaterThan(String topicName, Long messageId, int size,
      int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject gt = BasicDBObjectBuilder.start()
        .add(QueryOperators.GT, MongoUtil.longToBSONTimestamp(messageId)).get();
    DBObject query = BasicDBObjectBuilder.start().add(NID, gt).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBCursor cursor = collection.find(query).sort(orderBy).limit(size);

    List<KuroroMessage> list = new ArrayList<KuroroMessage>();
    try {
      while (cursor.hasNext()) {
        DBObject result = cursor.next();
        KuroroMessage message = new KuroroMessage();
        try {
          convert(result, message);
          list.add(message);
        } catch (RuntimeException e) {
          LOG.error("Error when convert resultset to kuroroMessage!", e);
        }
      }
    } finally {
      cursor.close();
    }
    return list;
  }

  @SuppressWarnings({"unchecked"})
  private void convert(DBObject result, KuroroMessage kuroroMessage) {
    BSONTimestamp timestamp = (BSONTimestamp) result.get(NID);
    kuroroMessage.setMessageId(MongoUtil.BSONTimestampToLong(timestamp));
    kuroroMessage.setContent(result.get(CONTENT));
    kuroroMessage.setVersion((String) result.get(VERSION));
    kuroroMessage.setGeneratedTime((Date) result.get(GENERATED_TIME));
    kuroroMessage.setCreatedTime((String) result.get(CREATED_TIME));
    kuroroMessage.setIdcName((String) result.get(IDC));
    kuroroMessage.setZone((String) result.get(ZONE));
    kuroroMessage.setPoolId((String) result.get(POOLID));

    Map<String, String> propertiesBasicDBObject = (Map<String, String>) result.get(PROPERTIES);
    if (propertiesBasicDBObject != null) {
      HashMap<String, String> properties = new HashMap<String, String>(propertiesBasicDBObject);
      kuroroMessage.setProperties(properties);
    }
    Map<String, String> internalPropertiesBasicDBObject = (Map<String, String>) result
        .get(INTERNAL_PROPERTIES);
    if (internalPropertiesBasicDBObject != null) {
      HashMap<String, String> properties = new HashMap<String, String>(
          internalPropertiesBasicDBObject);
      kuroroMessage.setInternalProperties(properties);
    }
    kuroroMessage.setSha1((String) result.get(SHA1));
    kuroroMessage.setType((String) result.get(TYPE));
    kuroroMessage.setSourceIp((String) result.get(SOURCE_IP));

    String protocolType = (String) result.get(PROTOCOLTYPE);
    if (protocolType != null) {
      kuroroMessage.setProtocolType(protocolType);
    }

  }

  @Override
  public void saveMessage(String topicName, KuroroMessage kuroroMessage, int level) {
    saveMessage(topicName, kuroroMessage, 0, level);
  }

  @Override
  public void saveMessage(String topicName, KuroroMessage message, int index, int level) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);
    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add(NID, new BSONTimestamp());
    convert(message, builder);
    WriteConcern writeConcern = null;
    switch (level) {
      case 0:
        writeConcern = WriteConcern.UNACKNOWLEDGED;
        break;
      case 1:
        writeConcern = WriteConcern.ACKNOWLEDGED;
        break;
      case 2:
        writeConcern = WriteConcern.JOURNALED;
        break;
      case 3:
        writeConcern = WriteConcern.MAJORITY;
        break;
      case 4:
        writeConcern = WriteConcern.W1;
        break;
      case 5:
        writeConcern = WriteConcern.W2;
        break;
      case 6:
        writeConcern = WriteConcern.W3;
        break;
      default:
        writeConcern = WriteConcern.UNACKNOWLEDGED;
    }
    if (level >= 1) {
      try {
        collection.insert(builder.get(), writeConcern);
      } catch (MongoException e) {
        LOG.error("save transfer message is error! ", e);
        throw e;
      }
    } else {
      collection.insert(builder.get(), writeConcern);
    }
  }

  @Override
  public Long getMessageIDGreaterThan(String topicName, Long messageId, int size) {
    return getMessageIDGreaterThan(topicName, messageId, size, 0);
  }

  @Override
  public Long getMessageIDGreaterThan(String topicName, Long messageId, int size, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);
    DBObject gt = BasicDBObjectBuilder.start()
        .add(QueryOperators.GT, MongoUtil.longToBSONTimestamp(messageId)).get();
    DBObject query = BasicDBObjectBuilder.start().add(NID, gt).get();
    DBObject fields = BasicDBObjectBuilder.start().add(NID, 1).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBCursor cursor = collection.find(query, fields).sort(orderBy).limit(size);
    try {
      BSONTimestamp timestamp = null;
      int count = cursor.size();
      if (count > 0) {
        timestamp = (BSONTimestamp) cursor.skip(count - 1).next().get(NID);
      }

      if (timestamp != null) {
        return MongoUtil.BSONTimestampToLong(timestamp);
      }
    } catch (Exception e) {
      if (e.getMessage().contains("Timeout while receiving message")) {
        LOG.warn(e.getMessage());
      } else {
        LOG.error(e.getMessage(), e);
      }
    } finally {
      cursor.close();
    }
    return null;
  }

  @Override
  public List<KuroroMessage> getMessagesGreaterThan(String topicName, Long minMessageId,
      Long maxMessageId,
      MessageFilter messageFilter) {
    return getMessagesGreaterThan(topicName, minMessageId, maxMessageId, messageFilter, 0, null);
  }

  @Override
  public List<KuroroMessage> getMessagesGreaterThan(String topicName, Long minMessageId,
      Long maxMessageId,
      MessageFilter messageFilter, int index, MessageFilter consumeLocalZoneFilter) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject gt = BasicDBObjectBuilder.start()
        .add(QueryOperators.GT, MongoUtil.longToBSONTimestamp(minMessageId))
        .add(QueryOperators.LTE, MongoUtil.longToBSONTimestamp(maxMessageId)).get();

    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add(NID, gt);

    if (messageFilter != null) {
      Set<String> words = messageFilter.getParam();
      if (words != null && !words.isEmpty()) {
        // 添加过滤逻辑
        builder.add(TYPE, new BasicDBObject(QueryOperators.IN, words));
      }
    }

    if (consumeLocalZoneFilter != null) {
      Set<String> zoneWord = consumeLocalZoneFilter.getParam();
      if (zoneWord != null && !zoneWord.isEmpty()) {
        builder.add(ZONE, new BasicDBObject(QueryOperators.IN, zoneWord));
      }
    }

    DBObject query = builder.get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBCursor cursor = collection.find(query).sort(orderBy);
    List<KuroroMessage> list = new ArrayList<KuroroMessage>();
    try {
      while (cursor.hasNext()) {
        DBObject result = cursor.next();
        KuroroMessage kuroroMessage = new KuroroMessage();
        try {
          convert(result, kuroroMessage);
          list.add(kuroroMessage);
        } catch (RuntimeException e) {
          LOG.error("Error when convert resultset to kuroroMessage.", e);
        }
      }
    } finally {
      cursor.close();
    }
    return list;
  }

  @Override
  public List<Long> getMessagesIdGreaterThan(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter) {
    return getMessagesIdGreaterThan(topicName, minMessageId, maxMessageId, messageFilter, 0);
  }

  @Override
  public List<Long> getMessagesIdGreaterThan(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);

    DBObject gt = BasicDBObjectBuilder.start()
        .add(QueryOperators.GT, MongoUtil.longToBSONTimestamp(minMessageId))
        .add(QueryOperators.LTE, MongoUtil.longToBSONTimestamp(maxMessageId)).get();

    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add(NID, gt);

    if (messageFilter != null) {
      Set<String> words = messageFilter.getParam();
      if (words != null && !words.isEmpty()) {
        // 添加过滤逻辑
        builder.add(TYPE, new BasicDBObject(QueryOperators.IN, words));
      }
    }

    DBObject query = builder.get();
    DBObject fields = BasicDBObjectBuilder.start().add(NID, 1).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBCursor cursor = collection.find(query, fields).sort(orderBy);
    List<Long> list = new ArrayList<Long>();
    try {
      while (cursor.hasNext()) {
        DBObject result = cursor.next();
        try {
          list.add(MongoUtil.BSONTimestampToLong((BSONTimestamp) result.get(NID)));
        } catch (RuntimeException e) {
          LOG.error("Error when convert resultset to kuroroMessage.", e);
        }
      }
    } finally {
      cursor.close();
    }

    return list;
  }

  @Override
  public List<Long> getMessageIdBetween(String topic, Long min, Long max) {
    return getMessageIdBetween(topic, min, max, 0);
  }

  @Override
  public List<Long> getMessageIdBetween(String topic, Long min, Long max, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topic, index);
    DBObject cond = BasicDBObjectBuilder.start()
        .add(QueryOperators.GTE, MongoUtil.longToBSONTimestamp(min))
        .add(QueryOperators.LT, MongoUtil.longToBSONTimestamp(max)).get();

    DBObject query = BasicDBObjectBuilder.start().add(NID, cond).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBObject keys = new BasicDBObject(NID, 1);
    DBCursor cursor = collection.find(query, keys).sort(orderBy);
    List<Long> ret = new ArrayList<Long>();
    while (cursor.hasNext()) {
      BSONTimestamp bsonTimestamp = (BSONTimestamp) cursor.next().get(NID);
      ret.add(MongoUtil.BSONTimestampToLong(bsonTimestamp));
    }
    return ret;
  }

  @Override
  public Long getMinIdAfter(String topic, BSONTimestamp bsonTimestamp) {
    return getMinIdAfter(topic, bsonTimestamp, 0);
  }

  @Override
  public Long getMinIdAfter(String topic, BSONTimestamp bsonTimestamp, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topic, index);

    DBObject lt = BasicDBObjectBuilder.start().add(QueryOperators.GTE, bsonTimestamp).get();
    DBObject query = BasicDBObjectBuilder.start().add(NID, lt).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBObject keys = new BasicDBObject(NID, 1);
    DBCursor cursor = collection.find(query, keys).limit(1).sort(orderBy);
    if (cursor.hasNext()) {
      DBObject result = cursor.next();
      return MongoUtil.BSONTimestampToLong((BSONTimestamp) result.get(NID));
    }

    return null;
  }

  @Override
  public int countBetween(String topic, Long until, Long newTil) {
    return countBetween(topic, until, newTil, 0);
  }

  @Override
  public int countBetween(String topic, Long until, Long newTil, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topic, index);
    DBObject cond = BasicDBObjectBuilder.start()
        .add(QueryOperators.LT, MongoUtil.longToBSONTimestamp(newTil))
        .add(QueryOperators.GTE, MongoUtil.longToBSONTimestamp(until)).get();

    DBObject query = BasicDBObjectBuilder.start().add(NID, cond).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(-1)).get();
    DBCursor cursor = collection.find(query).sort(orderBy);

    int count = cursor.count();
    return count;
  }

  @Override
  public Long getSkippedMessageId(String topic, Long until, int skip) {
    return getSkippedMessageId(topic, until, skip, 0);
  }

  @Override
  public Long getSkippedMessageId(String topic, Long until, int skip, int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topic, index);

    DBObject gt = BasicDBObjectBuilder.start()
        .add(QueryOperators.GTE, MongoUtil.longToBSONTimestamp(until)).get();
    DBObject query = BasicDBObjectBuilder.start().add(NID, gt).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBObject keys = new BasicDBObject(NID, 1);
    DBCursor cursor = collection.find(query, keys).skip(skip).limit(1).sort(orderBy);
    if (cursor.hasNext()) {
      DBObject result = cursor.next();
      return MongoUtil.BSONTimestampToLong((BSONTimestamp) result.get(NID));
    }

    return null;
  }

  @Override
  public List<KuroroMessage> getMessagesBetween(String topicName, Long minMessageId,
      Long maxMessageId,
      MessageFilter messageFilter, int index, MessageFilter consumeMessageFilter) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);
    DBObject gt = BasicDBObjectBuilder.start()
        .add(QueryOperators.GTE, MongoUtil.longToBSONTimestamp(minMessageId))
        .add(QueryOperators.LTE, MongoUtil.longToBSONTimestamp(maxMessageId)).get();

    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add(NID, gt);
    if (messageFilter != null) {
      Set<String> words = messageFilter.getParam();
      if (words != null && !words.isEmpty()) {
        // 添加过滤逻辑
        builder.add(TYPE, new BasicDBObject(QueryOperators.IN, words));
      }
    }

    if (consumeMessageFilter != null) {
      Set<String> zoneWord = consumeMessageFilter.getParam();
      if (zoneWord != null && !zoneWord.isEmpty()) {
        builder.add(ZONE, new BasicDBObject(QueryOperators.IN, zoneWord));
      }
    }

    DBObject query = builder.get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBCursor cursor = collection.find(query).sort(orderBy);
    List<KuroroMessage> list = new ArrayList<KuroroMessage>();
    try {
      while (cursor.hasNext()) {
        DBObject result = cursor.next();
        KuroroMessage message = new KuroroMessage();
        try {
          convert(result, message);
          list.add(message);
        } catch (RuntimeException e) {
          LOG.error("Error when convert resultset to kuroroMessage.", e);
        }
      }
    } finally {
      cursor.close();
    }
    return list;
  }

  @Override
  public List<Long> getMessageIdBetween(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter,
      int index) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);
    DBObject gt = BasicDBObjectBuilder.start()
        .add(QueryOperators.GTE, MongoUtil.longToBSONTimestamp(minMessageId))
        .add(QueryOperators.LTE, MongoUtil.longToBSONTimestamp(maxMessageId)).get();

    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add(NID, gt);
    if (messageFilter != null) {
      Set<String> words = messageFilter.getParam();
      if (words != null && !words.isEmpty()) {
        // 添加过滤逻辑
        builder.add(TYPE, new BasicDBObject(QueryOperators.IN, words));
      }
    }
    DBObject query = builder.get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
    DBObject keys = new BasicDBObject(NID, 1);
    DBCursor cursor = collection.find(query, keys).sort(orderBy);
    List<Long> list = new ArrayList<Long>();
    try {
      while (cursor.hasNext()) {
        DBObject result = cursor.next();
        try {
          list.add(MongoUtil.BSONTimestampToLong((BSONTimestamp) result.get(NID)));
        } catch (RuntimeException e) {
          LOG.error("Error when convert resultset to kuroroMessage.", e);
        }
      }
    } finally {
      cursor.close();
    }
    return list;
  }

  @Override
  public List<KuroroMessage> getMessagesBetween(String topicName, Long minMessageId,
      Long maxMessageId,
      MessageFilter messageFilter) {
    return getMessagesBetween(topicName, minMessageId, maxMessageId, messageFilter, 0, null);
  }

  @Override
  public List<Long> getMessageIdBetween(String topicName, Long minMessageId, Long maxMessageId,
      MessageFilter messageFilter) {
    return getMessageIdBetween(topicName, minMessageId, maxMessageId, messageFilter, 0);
  }

  @Override
  public void transferMessage(String topicName, KuroroMessage message, int level) {
    transferMessage(topicName, message, 0, level);
  }

  @Override
  public void transferMessage(String topicName, KuroroMessage message, int index, int level) {
    DBCollection collection = this.newMongoClient.getMessageCollection(topicName, index);
    // transfer来的消息，使用新的ID，将原ID放入OID字段
    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start()
        .add(OID, MongoUtil.longToBSONTimestamp(message.getMessageId()))
        .add(NID, new BSONTimestamp());
    convert(message, builder);
    WriteConcern writeConcern = null;
    switch (level) {
      case 0:
        writeConcern = WriteConcern.UNACKNOWLEDGED;
        break;
      case 1:
        writeConcern = WriteConcern.ACKNOWLEDGED;
        break;
      case 2:
        writeConcern = WriteConcern.JOURNALED;
        break;
      case 3:
        writeConcern = WriteConcern.MAJORITY;
        break;
      case 4:
        writeConcern = WriteConcern.W1;
        break;
      case 5:
        writeConcern = WriteConcern.W2;
        break;
      case 6:
        writeConcern = WriteConcern.W3;
        break;
      default:
        writeConcern = WriteConcern.UNACKNOWLEDGED;
    }
    if (level >= 1) {
      try {
        collection.insert(builder.get(), writeConcern);
      } catch (MongoException e) {
        LOG.error("save transfer message is error! " + topicName, e);
        throw e;
      }
    } else {
      collection.insert(builder.get(), writeConcern);
    }
  }

  private void convert(KuroroMessage message, BasicDBObjectBuilder builder) {
    // content
    String content = message.getContent();
    if (content != null && !"".equals(content.trim())) {
      builder.add(CONTENT, content);
    }
    // createTime
    String createdTime = message.getCreatedTime();
    if (createdTime != null) {
      builder.add(CREATED_TIME, createdTime);
    }
    // generatedTime
    Date generatedTime = message.getGeneratedTime();
    if (generatedTime != null) {
      builder.add(GENERATED_TIME, generatedTime);
    }
    // version
    String version = message.getVersion();
    if (version != null && !"".equals(version.trim())) {
      builder.add(VERSION, version);
    }
    // properties
    Map<String, String> properties = message.getProperties();
    if (properties != null && properties.size() > 0) {
      builder.add(PROPERTIES, properties);
    }
    // internalProperties
    Map<String, String> internalProperties = message.getInternalProperties();
    if (internalProperties != null && internalProperties.size() > 0) {
      builder.add(INTERNAL_PROPERTIES, internalProperties);
    }
    // sha1
    String sha1 = message.getSha1();
    if (sha1 != null && !"".equals(sha1.trim())) {
      builder.add(SHA1, sha1);
    }
    // type
    String type = message.getType();
    if (type != null && !"".equals(type.trim())) {
      builder.add(TYPE, type);
    }
    // sourceIp
    String sourceIp = message.getSourceIp();
    if (sourceIp != null && !"".equals(sourceIp.trim())) {
      builder.add(SOURCE_IP, sourceIp);
    }
    String protocolType = message.getProtocolType();
    if (protocolType != null) {
      builder.add(PROTOCOLTYPE, protocolType);
    }
    //idc
    String idc = message.getIdcName();
    if (idc != null) {
      builder.add(IDC, idc);
    }
    //zone
    String zone = message.getZone();
    if (zone != null) {
      builder.add(ZONE, zone);
    }
    //poolId
    String poolId = message.getPoolId();
    if (poolId != null) {
      builder.add(POOLID, poolId);
    }
  }
}

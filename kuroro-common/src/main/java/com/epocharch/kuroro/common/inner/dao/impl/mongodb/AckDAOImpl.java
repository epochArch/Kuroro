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

import com.epocharch.common.util.SystemUtil;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.epocharch.kuroro.common.inner.util.TimeUtil;
import com.epocharch.kuroro.common.jmx.support.JMXServiceURLBuilder;
import com.epocharch.zkclient.ZkClient;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckDAOImpl implements AckDAO {

  public static final String NID = "nid";
  public static final String SRC_CONSUMER_IP = "cip";
  public static final String TICK = "t";
  private static final Logger LOG = LoggerFactory.getLogger(AckDAOImpl.class);
  private final static int MONGO_ORDER_DESC = -1;
  private final static int MONGO_ORDER_ASC = 1;
  private final static String CONSUMERED_TIME = "ct";
  private static final String EMAIL_SUBJECT = "Duplicate LeaderServer Is Already Occurred !";
  private static final String JMX_OBJECT_NAME = "com.epocharch:name=LeaderClientManager-Topic-Info";
  private static final long EMAIL_SEND_FREQUENCY = 60L * 60L * 1000L;
  private static final int JMX_PORT = 3997;
  private static final AtomicLong lastCheckTime = new AtomicLong();
  private static final String IDC = "idc";
  private static final String ZONE = "ze";
  private static final String POOLID = "pd";
  private NewMongoClient newMongoClient;

  public AckDAOImpl() {
  }

  public void setNewMongoClient(
      NewMongoClient newMongoClient) {
    this.newMongoClient = newMongoClient;
  }


  @Override
  public Long getMaxMessageID(String topicName, String consumerId) {
    return getMaxMessageID(topicName, consumerId, 0, null);
  }

  @Override
  public boolean isAcked(String topicName, String consumerId, Long messageId, String zone) {
    return isAcked(topicName, consumerId, messageId, 0, zone);
  }

  @Override
  public boolean isAcked(String topicName, String consumerId, Long messageId, int index,
      String zone) {
    DBCollection collection = this.newMongoClient
        .getAckCollection(topicName, consumerId, index, zone);
    DBObject cond = new BasicDBObject();
    cond.put(NID, MongoUtil.longToBSONTimestamp(messageId));

    DBObject fields = new BasicDBObject();
    fields.put(NID, 1);
    Object obj = collection.findOne(cond, fields);
    return obj != null;
  }

  @Override
  public Long getMaxMessageID(String topicName, String consumerId, int index, String zone) {
    DBCollection collection = this.newMongoClient
        .getAckCollection(topicName, consumerId, index, zone);

    DBObject fields = BasicDBObjectBuilder.start().add(NID, 1).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, MONGO_ORDER_DESC).get();
    DBCursor cursor = collection.find(new BasicDBObject(), fields).sort(orderBy).limit(1);
    try {
      if (cursor.hasNext()) {
        DBObject result = cursor.next();
        BSONTimestamp timestamp = (BSONTimestamp) result.get(NID);
        return MongoUtil.BSONTimestampToLong(timestamp);
      }
    } finally {
      cursor.close();
    }
    return null;
  }

  @Override
  public void add(String topicName, String consumerId, Long messageId, String sourceConsumerIp) {
    add(topicName, consumerId, messageId, sourceConsumerIp, 0, null);
  }

  @Override
  public void add(String topicName, String consumerId, Long messageId, String sourceConsumerIp,
      int index, String zone) {

    DBCollection collection = this.newMongoClient
        .getAckCollection(topicName, consumerId, index, zone);
    BSONTimestamp timestamp = MongoUtil.longToBSONTimestamp(messageId);
    try {
      DBObject add = BasicDBObjectBuilder.start().add(NID, timestamp)
          .add(SRC_CONSUMER_IP, sourceConsumerIp).
              add(CONSUMERED_TIME, TimeUtil.getNowTime()).add(ZONE, zone).get();
      collection.insert(add);
    } catch (MongoException e) {
      if (e.getMessage() != null && e.getMessage().indexOf("duplicate key") >= 0
          || e.getCode() == 12582 || e.getCode() == 11000) {
        // nid already exists
        LOG.warn(
            e.getMessage() + ": " + NID + " is " + timestamp + ",topic=" + topicName + ",consumer="
                + consumerId);
        checkMultipleLeader(topicName, consumerId, sourceConsumerIp);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void add(String topicName, String consumerId, Long messageId, String sourceConsumerIp,
      int index, String idc,
      String zone, String poolId) {

    DBCollection collection = this.newMongoClient
        .getAckCollection(topicName, consumerId, index, zone);
    BSONTimestamp timestamp = MongoUtil.longToBSONTimestamp(messageId);
    try {
      DBObject add = BasicDBObjectBuilder.start().add(NID, timestamp)
          .add(SRC_CONSUMER_IP, sourceConsumerIp)
          .add(CONSUMERED_TIME, TimeUtil.getNowTime()).add(IDC, idc).add(ZONE, zone)
          .add(POOLID, poolId).get();
      collection.insert(add);
    } catch (MongoException e) {
      if (e.getMessage() != null && e.getMessage().indexOf("duplicate key") >= 0
          || e.getCode() == 12582 || e.getCode() == 11000) {
        // nid already exists
        LOG.warn(
            e.getMessage() + ": " + NID + " is " + timestamp + ",topic=" + topicName + ",consumer="
                + consumerId);
        checkMultipleLeader(topicName, consumerId, sourceConsumerIp);
      } else {
        throw e;
      }
    }
  }

  @Override
  public int countBetween(String topic, String consumer, Long until, Long newTil) {
    return countBetween(topic, consumer, until, newTil, 0, null);
  }

  @Override
  public int countBetween(String topic, String consumer, Long until, Long newTil, int index,
      String zone) {
    DBCollection collection = this.newMongoClient.getAckCollection(topic, consumer, index, zone);
    DBObject cond = BasicDBObjectBuilder.start().add("$lt", MongoUtil.longToBSONTimestamp(newTil))
        .add("$gte", MongoUtil.longToBSONTimestamp(until)).get();

    DBObject query = BasicDBObjectBuilder.start().add(NID, cond).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(-1)).get();
    DBCursor cursor = collection.find(query).sort(orderBy);
    int count = cursor.count();
    return count;
  }

  @Override
  public Long getMinMessageID(String topicName, String consumerId, String zone) {
    return getMinMessageID(topicName, consumerId, 0, zone);
  }

  @Override
  public Long getMinMessageID(String topicName, String consumerId, int index, String zone) {
    DBCollection collection = this.newMongoClient
        .getAckCollection(topicName, consumerId, index, zone);

    DBObject fields = BasicDBObjectBuilder.start().add(NID, 1).get();
    DBObject orderBy = BasicDBObjectBuilder.start().add(NID, MONGO_ORDER_ASC).get();
    DBCursor cursor = collection.find(new BasicDBObject(), fields).sort(orderBy).limit(1);
    try {
      if (cursor.hasNext()) {
        DBObject result = cursor.next();
        BSONTimestamp timestamp = (BSONTimestamp) result.get(NID);
        return MongoUtil.BSONTimestampToLong(timestamp);
      }
    } finally {
      cursor.close();
    }
    return null;
  }

  private void checkMultipleLeader(final String topicName, final String consumerId,
      final String sourceConsumerIp) {
    long lastCheckTimeMillis = lastCheckTime.get();
    final long startTimeMillis = System.currentTimeMillis();
    if (startTimeMillis - lastCheckTimeMillis > EMAIL_SEND_FREQUENCY) {
      boolean isSucceed = lastCheckTime.compareAndSet(lastCheckTimeMillis, startTimeMillis);
      if (isSucceed && isExistMultipleLeader()) {
        new Thread(new Runnable() {
          @SuppressWarnings("static-access")
          @Override
          public void run() {
            while (System.currentTimeMillis() - startTimeMillis < 6L * 60L * 1000L) {
              if (isExistMultipleLeader()) {
                try {
                  Thread.currentThread().sleep(60L * 1000L);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              } else {
                return;
              }
            }
            StringBuilder emailContent = new StringBuilder("topic name : ");
            emailContent.append(topicName).append(", consumerId : ").append(consumerId)
                .append(", consumer serverIp : ")
                .append(SystemUtil.getLocalhostIp()).append(", source consumerIp : ")
                .append(sourceConsumerIp)
                .append(", happen time").append(new Date(startTimeMillis).toString());
            KuroroUtil.sendEmail(Constants.SEND_EMAIL_LIST, emailContent.toString(), EMAIL_SUBJECT);
          }
        }).start();
      }
    }
  }

  private boolean isExistMultipleLeader() {
    String brokerGroup = System
        .getProperty("brokerGroup", Constants.KURORO_DEFAULT_BROKER_GROUPNAME);
    String consumerServerPath =
        System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH) + Constants.ZONE_BROKER
            + Constants.SEPARATOR + brokerGroup
            + Constants.ZONE_BROKER_CONSUMER;
    try {
      ZkClient _zkClient = KuroroZkUtil.initIDCZk();
      List<String> consumerHostList = _zkClient.getChildren(consumerServerPath);
      if (consumerHostList != null && consumerHostList.size() > 1) {
        List<String> sortBrokerList = new ArrayList<String>();
        List<String> sortTopicList = new ArrayList<String>();
        for (String host : consumerHostList) {
          JMXServiceURLBuilder builder = new JMXServiceURLBuilder(host, JMX_PORT);
          JMXConnector conn = JMXConnectorFactory.connect(builder.getJMXServiceURL());
          MBeanServerConnection mbeanServerConnection = conn.getMBeanServerConnection();
          ObjectName objectName = new ObjectName(JMX_OBJECT_NAME);
          Object sortBrokers = mbeanServerConnection.getAttribute(objectName, "SortBrokers");
          if (sortBrokers != null && !sortBrokers.toString().isEmpty()) {
            sortBrokerList.add(sortBrokers.toString());
          }
          Object sortTopics = mbeanServerConnection.getAttribute(objectName, "SortTopics");
          if (sortTopics != null && !sortTopics.toString().isEmpty()) {
            sortTopicList.add(sortTopics.toString());
          }
        }
        if (sortBrokerList.size() > 1) {
          String sortBrokerString = sortBrokerList.get(0);
          for (int i = 1; i < sortBrokerList.size(); i++) {
            if (!sortBrokerString.equals(sortBrokerList.get(i))) {
              return true;
            }
          }
        }
        if (sortTopicList.size() > 1) {
          String sortTopicString = sortTopicList.get(0);
          for (int i = 1; i < sortTopicList.size(); i++) {
            if (!sortTopicString.equals(sortTopicList.get(i))) {
              return true;
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("MultipleLeader checking is happen exception : " + e.getMessage(), e);
      return true;
    }

    return false;
  }
}

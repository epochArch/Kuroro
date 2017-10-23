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

import com.epocharch.kuroro.common.inner.config.ConfigChangeListener;
import com.epocharch.kuroro.common.inner.config.DynamicConfig;
import com.epocharch.kuroro.common.inner.config.Operation;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.config.impl.TopicDynamicConfig;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.jmx.support.JmxSpringUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

public class NewMongoClient implements ConfigChangeListener {

  private static final Logger LOG = LoggerFactory.getLogger(NewMongoClient.class);
  private static final String DEFAULT_COLLECTION_NAME = "c";
  private static final String COMPENSATION_DB_NAME = "compensation";
  private static final String MILCONFIGCENTER = "000000";
  private final Map<String, Byte> collectionExistsSign = new ConcurrentHashMap<String, Byte>();
  private final MongoConfig config;
  private final String NID = "nid";
  private Map<String, Integer> msgTopicNameToSizes = new ConcurrentHashMap<String, Integer>();
  private Map<String, Integer> ackTopicNameToSizes = new ConcurrentHashMap<String, Integer>();
  private Map<String, List<MongoClient>> topicNameToMongoMap = new ConcurrentHashMap<String, List<MongoClient>>();
  private Map<String, List<MongoClient>> topicNameToCompenMongoMap = new ConcurrentHashMap<String, List<MongoClient>>();
  private JmxSpringUtil jmxSpringUtil;
  private DynamicConfig dynamicConfig;
  private MongoClientOptions mongoClientOptions = null;
  private List<MongoCredential> credentList = new ArrayList<MongoCredential>();

  public NewMongoClient() {
    config = new MongoConfig();
    mongoClientOptions = getMongoClientOptions(config);
    LOG.info(
        (new StringBuilder()).append("mongoClientOptions=").append(mongoClientOptions.toString())
            .toString());
    this.dynamicConfig = new TopicDynamicConfig();
    //Mongodb parameter init
    char[] pwd_char = config.getPassWord().toCharArray();
    MongoCredential credent = MongoCredential
        .createCredential(config.getUserName(), "admin", pwd_char);
    credentList.add(credent);
    loadConfig();
    if (LOG.isDebugEnabled()) {
      LOG.debug("NewMongoClient done...");
    }

  }

  private void loadConfig() {
    try {
      createTopicMongoSets(this.dynamicConfig.getKeySet());
      this.dynamicConfig.setConfigChangeListener(this);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          (new StringBuilder()).append("Error Loading Config from kuroro of ZK : ")
              .append(e.getMessage()).toString(), e);
    }
  }

  @Override
  public synchronized void onConfigChange(String key, Object value, Operation oper) {
    if (LOG.isInfoEnabled()) {
      LOG.info("onChange() called.");
    }
    if (value instanceof TopicConfigDataMeta) {
      if (!oper.equals(Operation.DELETE)) {
        try {
          TopicConfigDataMeta data = (TopicConfigDataMeta) value;
          createTopicMongo(data);
        } catch (Exception e) {
          LOG.error((new StringBuilder())
              .append("Error occour when reset config from ZK, no config property would changed :")
              .append(e.getMessage()).toString(), e);
        }
      } else {
        // remove
        List<MongoClient> unUseMongList = this.topicNameToMongoMap.get(key);
        List<MongoClient> unUseMongCompenList = this.topicNameToCompenMongoMap.get(key);
        this.topicNameToMongoMap.remove(key);
        this.topicNameToCompenMongoMap.remove(key);
        this.msgTopicNameToSizes.remove(key);
        this.ackTopicNameToSizes.remove(key);
        if (unUseMongList != null) {
          for (MongoClient mg : unUseMongList) {
            MongoClient g = getExistsMongoClient(mg.getAllAddress(), this.topicNameToMongoMap);
            if (g == null) {
              closeUnuseMongoClient(mg);
            }
          }
        }
        if (unUseMongCompenList != null) {
          for (MongoClient mg : unUseMongCompenList) {
            MongoClient g = getExistsMongoClient(mg.getAllAddress(),
                this.topicNameToCompenMongoMap);
            if (g == null) {
              closeUnuseMongoClient(mg);
            }
          }
        }
      }
    }
  }

  private void closeUnuseMongoClient(MongoClient unuseMongoClient) {
    if (unuseMongoClient != null) {
      unuseMongoClient.close();
      LOG.info("Close unuse MongoClient: " + unuseMongoClient);
    }

  }

  private void createTopicMongoSets(Set<String> keySet) {
    Iterator<String> it = keySet.iterator();
    if (it != null) {
      while (it.hasNext()) {
        String key = it.next();
        TopicConfigDataMeta data = (TopicConfigDataMeta) this.dynamicConfig.get(key);
        try {
          createTopicMongo(data);
        } catch (Exception e) {
          LOG.warn("load mongoIP data is fail! " + data, e);
          continue;
        }
      }
    }
  }

  private void createTopicMongo(TopicConfigDataMeta data) {
    if (data != null) {
      String key = data.getTopicName();
      msgTopicNameToSizes.put(key, data.getMessageCappedSize());
      ackTopicNameToSizes.put(key, data.getAckCappedSize());
      if (this.topicNameToMongoMap.containsKey(key)) {
        this.topicNameToMongoMap.remove(key);
      }
      if (this.topicNameToCompenMongoMap.containsKey(key)) {
        this.topicNameToCompenMongoMap.remove(key);
      }
      for (String uri : data.getReplicationSetList()) {
        if (KuroroUtil.isBlankString(uri)) {
          LOG.error("topic name: " + data.getTopicName()
              + " mongodb address is null, please check TopicConfigDataMeta object on zk.");
          continue;
        }
        List<ServerAddress> replicaSetSeeds = parseUriToAddressList(uri);
        //create 2 connection pool：1.get mongodb message read sencondary; 2. compenisation read primary
        MongoClient mongoClient = getExistsMongoClient(replicaSetSeeds, this.topicNameToMongoMap);
        if (config.isSlaveOk()) {
          mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
        }
        MongoClient mongoClientCompen = getExistsMongoClient(replicaSetSeeds,
            this.topicNameToCompenMongoMap);

        if (!this.topicNameToMongoMap.containsKey(key)) {
          List<MongoClient> mongoClientList = new ArrayList<MongoClient>();
          mongoClientList.add(mongoClient);
          this.topicNameToMongoMap.put(key, mongoClientList);
        } else {
          this.topicNameToMongoMap.get(key).add(mongoClient);
        }

        if (!this.topicNameToCompenMongoMap.containsKey(key)) {
          List<MongoClient> mongoClientList = new ArrayList<MongoClient>();
          mongoClientList.add(mongoClientCompen);
          this.topicNameToCompenMongoMap.put(key, mongoClientList);
        } else {
          this.topicNameToCompenMongoMap.get(key).add(mongoClientCompen);
        }
      }
    }
  }

  // mongoclient
  private MongoClient getExistsMongoClient(List<ServerAddress> replicaSetSeeds,
      Map<String, List<MongoClient>> topicNameMap) {
    MongoClient mongoClient = null;
    if (topicNameMap != null) {// 如果已有的map中已经存在该Mongo实例，则重复使用
      List<MongoClient> mongoList = mergeCollection(topicNameMap.values());
      for (MongoClient m : mongoList) {
        if (equalsOutOfOrder(m.getAllAddress(), replicaSetSeeds)) {
          mongoClient = m;
          break;
        }
      }
    }

    if (mongoClient != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("getExistsMongoClient() return a exists MongoClient instance : " + mongoClient
            .getServerAddressList() + "==="
            + mongoClient);
      }
    } else {
      mongoClient = new MongoClient(replicaSetSeeds, credentList, mongoClientOptions);
    }
    return mongoClient;
  }

  // mongoclient
  private List<MongoClient> mergeCollection(Collection<List<MongoClient>> collection) {
    List<MongoClient> list = new ArrayList<MongoClient>();
    Iterator<List<MongoClient>> it = collection.iterator();
    while (it.hasNext()) {
      list.addAll(it.next());
    }
    return list;
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

  @SuppressWarnings({"unchecked", "rawtypes"})
  private boolean equalsOutOfOrder(List list1, List list2) {
    if (list1 == null || list2 == null) {
      return false;
    } else
    // return list1.containsAll(list2) && list2.containsAll(list1);
    // 由于新的mongodb-driver的问题，去掉MONGODB集群中仲裁节点的对判断的影响
    {
      return list1.containsAll(list2);
    }
  }

  private MongoClientOptions getMongoClientOptions(MongoConfig config) {
    MongoClientOptions.Builder builder = new MongoClientOptions.Builder();

    builder.connectionsPerHost(config.getConnectionsPerHost());
    builder.socketKeepAlive(config.isSocketKeepAlive());
    builder.socketTimeout(config.getSocketTimeout());
    builder.threadsAllowedToBlockForConnectionMultiplier(
        config.getThreadsAllowedToBlockForConnectionMultiplier());
    builder.connectTimeout(config.getConnectTimeout());
    builder.maxWaitTime(config.getMaxWaitTime());
    builder.serverSelectionTimeout(config.getServerSelectionTimeout());
    MongoClientOptions options = builder.build();

    return options;
  }

  private int getSafeInt(Map<String, Integer> map, String key) {
    Integer i = null;
    if (map != null) {
      i = map.get(key);
    }
    return i != null ? i.intValue() : -1;
  }

  public DBCollection getMessageCollection(String topicName) {
    return getMessageCollection(topicName, 0);
  }

  public DBCollection getMessageCollection(String topicName, int index) {
    MongoClient mongoClient = null;
    List<MongoClient> mongolist = topicNameToMongoMap.get(topicName);
    if (mongolist == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            (new StringBuilder()).append("topicname '").append(topicName)
                .append("' do not match any Mongo Server,use default.")
                .toString());
      }
    } else {
      if (mongolist.size() > 0) {
        mongoClient = mongolist.get(index);
      }
    }

    // 前方深坑
    // fix a problem that transferred topic name is too long
    if (topicName != null && topicName.indexOf("@") != -1) {// 确定是transferred的topic
      // 长度限制64
      String midName = null;
      if (topicName.contains("@ZONE_")) {
        midName = StringUtils.remove(topicName, "@ZONE_");
      } else {
        midName = topicName;
      }
      midName = StringUtils.remove(midName, "@");
      if (midName.length() > 61) {
        midName = DigestUtils.md5Hex(midName);
      }
      return getCollection(mongoClient, getSafeInt(msgTopicNameToSizes, topicName), "m_", midName,
          new BasicDBObject(NID, Integer.valueOf(-1)));
    } else {
      return getCollection(mongoClient, getSafeInt(msgTopicNameToSizes, topicName), "msg_",
          topicName,
          new BasicDBObject(NID, Integer.valueOf(-1)));
    }
  }

  private DBCollection getCollection(MongoClient mongoClient, int size, String dbNamePrefix,
      String topicName, DBObject indexDBObject) {
    String dbName = (new StringBuilder()).append(dbNamePrefix).append(topicName).toString();
    DB db = mongoClient.getDB(dbName);
    DBCollection collection = null;
    String collectionKey = db.toString() + mongoClient.getServerAddressList().toString();
    if (!collectionExists(collectionKey)) {
      synchronized (db) {
        try {
          boolean collectionExistFlag = db.collectionExists(DEFAULT_COLLECTION_NAME);
          boolean indexExists = false;
          if (collectionExistFlag) {
            collection = db.getCollection(DEFAULT_COLLECTION_NAME);
            List<DBObject> indexList = collection.getIndexInfo();
            for (DBObject obj : indexList) {
              if (obj.get("name") != null && "nid_-1".equals(obj.get("name"))) {
                indexExists = true;
                break;
              }
            }
            if (indexList != null) {
              indexList.clear();
              indexList = null;
            }
          }

          if (!indexExists || !collectionExistFlag) {
            collection = createCollection(db, DEFAULT_COLLECTION_NAME, size, indexDBObject);
          }
          remarkCollectionExists(collectionKey);
        } catch (MongoException e) {
          if (e.getMessage().indexOf("Timeout while receiving message") >= 0) {
            LOG.error("create " + db + " index may be fail! try again! ", e.getMessage());
            try {
              collection = createCollection(db, DEFAULT_COLLECTION_NAME, size, indexDBObject);
              remarkCollectionExists(collectionKey);
            } catch (MongoException e2) {
              LOG.error(db + " create collection may be fail ! ", e2);
            }
          }
        }
      }
      if (collection == null) {
        collection = db.getCollection(DEFAULT_COLLECTION_NAME);
      }
    } else {
      collection = db.getCollection(DEFAULT_COLLECTION_NAME);
    }
    return collection;

  }

  /**
   * 通过指定的topic，获得其所在replicaset上的补偿collection
   */
  public DBCollection getCompensationCollection(String topic) {
    MongoClient mongoClient = null;
    List<MongoClient> mongolist = topicNameToCompenMongoMap.get(topic);
    if (mongolist == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug((new StringBuilder()).append("topicname ' ").append(topic)
            .append("' do not match any Mongo Server,use default.")
            .toString());
      }
    } else {
      mongoClient = mongolist.get(0);
    }
    DB db = mongoClient.getDB(COMPENSATION_DB_NAME);

    boolean isCollectionExist = db.collectionExists(DEFAULT_COLLECTION_NAME);
    DBCollection collection = null;
    if (!isCollectionExist) {// id,topic,consumer,
      collection = db.createCollection(DEFAULT_COLLECTION_NAME, new BasicDBObject());
      // index obj
      DBObject idxObj = new BasicDBObject();
      idxObj.put(CompensationDaoImpl.COL_TOPIC, 1);
      collection.createIndex(idxObj);
    } else {
      collection = db.getCollection(DEFAULT_COLLECTION_NAME);
    }

    return collection;

  }

  private boolean collectionExists(String key) {
    return collectionExistsSign.containsKey(key);
  }

  private void remarkCollectionExists(String key) {
    collectionExistsSign.put(key, Byte.valueOf((byte) 127));
  }

  private DBCollection createCollection(DB db, String collectionName, int size,
      DBObject indexDBObject) {
    DBObject options = new BasicDBObject();
    options.put("capped", Boolean.valueOf(true));
    DBCollection collection = null;
    if (size > 0) {
      options.put("size", Long.parseLong(String.valueOf(size) + MILCONFIGCENTER));
    }
    try {
      collection = db.createCollection(collectionName, options);
      LOG.info((new StringBuilder()).append("Create collection'").append(collectionName)
          .append("' on db ").append(db)
          .append(",index is ").append(indexDBObject).toString());
    } catch (MongoException e) {
      if (e.getMessage() != null && e.getMessage().indexOf("collection already exists") >= 0) {
        LOG.warn((new StringBuilder()).append(e.getMessage()).append(":the collectionName is ")
            .append(collectionName).toString());
        collection = db.getCollection(collectionName);
      } else {
        throw e;
      }
    } finally {
      if (indexDBObject != null) {
        collection.createIndex(indexDBObject);
        LOG.info((new StringBuilder()).append("Create index").append(indexDBObject)
            .append(" on collection ").append(collection)
            .toString());
      }
      return collection;
    }
  }

  public DBCollection getAckCollection(String topicName, String consumerId) {
    return getAckCollection(topicName, consumerId, 0, null);
  }

  public DBCollection getAckCollection(String topicName, String consumerId, int index,
      String zone) {
    List<MongoClient> mongoClientList = topicNameToMongoMap.get(topicName);
    MongoClient mongoClient = null;
    if (mongoClientList == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            (new StringBuilder()).append("topicName '").append(topicName)
                .append(" 'do not match any Mongo Server,use default.")
                .toString());
      }
    } else {
      if (mongoClientList.size() > 0) {
        mongoClient = mongoClientList.get(index);
      }
    }
    // fix a problem that transferred topic name is too long
    if (topicName != null && topicName.indexOf("@") != -1) {// 确定是transferred的topic
      // 长度限制63
      String midName = null;
      if (topicName.contains("@ZONE_")) {
        midName = StringUtils.remove(topicName, "@ZONE_");
      } else {
        midName = topicName;
      }
      midName = StringUtils.remove(midName, "@");
      if (midName.length() > 63) {
        midName = DigestUtils.md5Hex(midName);
      }
      LOG.debug("Reduce topic name length {}:{}", topicName, midName);
      return getCollection(mongoClient, getSafeInt(ackTopicNameToSizes, topicName), "", midName,
          new BasicDBObject(NID, Integer.valueOf(-1)));
    } else {
      // 填之前的坑，深坑
      String prefix = "ack_";
      if (topicName.length() + consumerId.length() > 58) {
        prefix = "a_";
      }
      StringBuilder ackName = new StringBuilder();
      ackName.append(topicName).append("#").append(consumerId);
      if (!KuroroUtil.isBlankString(zone)) {
        ackName.append("#").append(zone);
      }
      return getCollection(mongoClient, getSafeInt(ackTopicNameToSizes, topicName), prefix,
          ackName.toString(),
          new BasicDBObject(NID, Integer.valueOf(-1)));
    }
  }

  public Map<String, List<MongoClient>> getTopicNameToMongoMap() {
    return topicNameToMongoMap;
  }

  public JmxSpringUtil getJmxSpringUtil() {
    return jmxSpringUtil;
  }

  public void setJmxSpringUtil(JmxSpringUtil jmxSpringUtil) {
    this.jmxSpringUtil = jmxSpringUtil;
    // Monitor
    jmxSpringUtil.registerMBean("NewMongoClient", new MonitorBean(this));
  }

  public MongoClientOptions getMongoClientOptions() {
    return mongoClientOptions;
  }

  @ManagedResource(description = "newmongoclient info in running")
  public static class MonitorBean {

    private final WeakReference<NewMongoClient> newMongoClient;

    private MonitorBean(NewMongoClient client) {
      this.newMongoClient = new WeakReference<NewMongoClient>(client);
    }

    @ManagedAttribute
    public String getMongoOptions() {
      return (newMongoClient.get() != null) ? newMongoClient.get().mongoClientOptions.toString()
          : null;
    }

    @ManagedAttribute
    public String getCollectionExistsSign() {
      return (newMongoClient.get() != null) ? newMongoClient.get().collectionExistsSign.toString()
          : null;
    }

    @ManagedAttribute
    public String getTopicNameToMongoMap() {
      return (newMongoClient.get() != null) ? newMongoClient.get().topicNameToMongoMap.toString()
          : null;
    }
  }
}

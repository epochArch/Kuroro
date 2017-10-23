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

import com.epocharch.kuroro.common.inner.dao.CompensationDao;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bill
 * @date 4/30/14
 */
public class CompensationDaoImpl implements CompensationDao {

  private static final Logger logger = LoggerFactory.getLogger(CompensationDaoImpl.class);
  public static final String COL_TOPIC = "t";
  public static final String COL_CONSUMER = "c";
  public static final String COL_UNTIL_ID = "i";
  public static final String NID = "nid";
  public static final String COL_ZONE = "z";
  private NewMongoClient newMongoClient;


  public void setNewMongoClient(
      NewMongoClient newMongoClient) {
    this.newMongoClient = newMongoClient;
  }

  public CompensationDaoImpl() {
    ;
  }

  @Override
  public BSONTimestamp getCompensationId(String topic, String consumerId, String zone) {
    DBCollection collection = newMongoClient.getCompensationCollection(topic);

    DBObject condition = new BasicDBObject();
    condition.put(COL_TOPIC, topic);
    condition.put(COL_CONSUMER, consumerId);
    if (!KuroroUtil.isBlankString(zone)) {
      condition.put(COL_ZONE, zone);
    }

    DBObject field = new BasicDBObject();
    field.put(NID, 1);
    field.put(COL_UNTIL_ID, 1);
    if (!KuroroUtil.isBlankString(zone)) {
      condition.put(COL_ZONE, 1);
    }

    DBObject result = collection.findOne(condition, field);
    if (result == null) {
      return null;
    } else {
      Object idx = result.get(COL_UNTIL_ID);

      if (idx != null) {
        return (BSONTimestamp) idx;
      }
    }
    return null;
  }

  @Override
  public DBObject updateCompensationId(String topic, String consumerId, BSONTimestamp id,
      String zone) {
    DBCollection collection = newMongoClient.getCompensationCollection(topic);

    DBObject condition = new BasicDBObject();
    condition.put(COL_TOPIC, topic);
    condition.put(COL_CONSUMER, consumerId);
    if (!KuroroUtil.isBlankString(zone)) {
      condition.put(COL_ZONE, zone);
    }

    DBObject update = new BasicDBObject();
    update.put(COL_TOPIC, topic);
    update.put(COL_CONSUMER, consumerId);
    update.put(COL_UNTIL_ID, id);
    if (!KuroroUtil.isBlankString(zone)) {
      update.put(COL_ZONE, zone);
    }

    return collection.findAndModify(condition, null, null, false, update, true, true);

  }

  @Override
  public void deleteCompensation(String topic, String consumerId, String zone) {
    DBCollection collection = newMongoClient.getCompensationCollection(topic);
    DBObject condition = new BasicDBObject();
    condition.put(COL_TOPIC, topic);
    condition.put(COL_CONSUMER, consumerId);
    if (!KuroroUtil.isBlankString(zone)) {
      condition.put(COL_ZONE, zone);
    }

    collection.findAndRemove(condition);
  }

}

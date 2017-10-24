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


package com.epocharch.kuroro.common.mongo;


import static com.epocharch.kuroro.common.inner.util.MongoUtil.BSONTimestampToLong;

import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.QueryOperators;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;
import org.junit.Test;

/**
 * @author zfq
 *
 */
public class MongoDBQueryTest {
	
	/**
	 * 
	 */
	private String mongodbURL = "10.161.144.68";
	private int mongodbport = 27017;
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
	
	
	public MongoDBQueryTest() {
		getIteratorValue();
		//sendEmail();
		//updateTest();
		//queryArrayTest();
		//queryIdTest();
		/*regexQueryTest();
		getConnectionNum();
		queryTimeStampTest();*/
	}
	
	
	public void getIteratorValue(){
		 ConcurrentHashMap<Integer, String> client = new ConcurrentHashMap<Integer,String>();

			client.put(1," one");
			client.put(2, "two");
			client.put(3, "three");
			
			Iterator<Entry<Integer, String>> it = null;
			while(client.size() > 0){
			if (client != null) {
				if (client != null && client.size() > 0) {
					it = client.entrySet().iterator();
					//if (it.hasNext()) {
						Map.Entry<Integer, String> entry = it.next();
						String value = entry.getValue();
						System.out.println( " get removeTimeOutMap pair.....topic#consumer >>> "+ entry.getKey()+"...."+entry.getValue());
						it.remove();
						System.out.println(client+"...."+value);
					//}
				}
			}
			}
			
		}
	
	public void getConnectionNum(){
		Bson bson = new Document("serverStatus", 1);
		System.out.println( connectTest().getDatabase("admin").runCommand(bson).get("connections"));
    	//System.out.println("mongoVersion.........."+serverVersion);
	}
	
	//时间戳查询
	public void queryIdTest(){
		DBCollection collection = connectTest().getDB("admin").getCollection("tests");
		
		DBObject fields = BasicDBObjectBuilder.start("points", 53.0).add("bonus", 15.0).get();
		/*DBObject cond = BasicDBObjectBuilder.start()
				.add("$in", fields).get();*/
		DBObject query = BasicDBObjectBuilder.start()
				.add("points", fields).get();
		DBObject orderBy = BasicDBObjectBuilder.start()
				.add("points", Integer.valueOf(1)).get();
		DBCursor cursor = collection.find(query).sort(orderBy).limit(3);
		List<KuroroMessage> list = new ArrayList<KuroroMessage>();
			while (cursor.hasNext()) {
				DBObject result = cursor.next();
				System.out.println("result"+result);
			}
	}
	
	//query mongodb arrays
	public void queryArrayTest(){
		DBCollection collection = connectTest().getDB("admin").getCollection("tests");
		
		DBObject fields = BasicDBObjectBuilder.start("bonus", 8.0).get();//.add("bonus", 15.0).get();
		DBObject cond = BasicDBObjectBuilder.start()
				.add("$elemMatch", fields).get();
		DBObject query = BasicDBObjectBuilder.start()
				.add("points", cond).get();
		DBObject orderBy = BasicDBObjectBuilder.start()
				.add("points", Integer.valueOf(1)).get();
		DBCursor cursor = collection.find(query).sort(orderBy).limit(3);
		List<KuroroMessage> list = new ArrayList<KuroroMessage>();
			while (cursor.hasNext()) {
				DBObject result = cursor.next();
				System.out.println("result"+result);
			}
	}
	
	
	
	//时间戳查询
	public void queryTimeStampTest(){
		DBCollection collection = connectTest().getDB("msg_gos_create_so").getCollection("c");
		DBObject query = BasicDBObjectBuilder.start().add("nid", MongoUtil.longToBSONTimestamp(6281458720589217798L)).get();
		DBObject orderBy = BasicDBObjectBuilder.start()
				.add("nid", Integer.valueOf(-1)).get();
		DBCursor cursor = collection.find(query).sort(orderBy).limit(5);
		
		List<KuroroMessage> list = new ArrayList<KuroroMessage>();
			while (cursor.hasNext()) {
				DBObject result = cursor.next();
				System.out.println("result"+result);
			}
	}
	
	//正则查询
	public void regexQueryTest(){
		DBCollection collection = connectTest().getDB("msg_gos_create_so").getCollection("c");
		DBObject gt = BasicDBObjectBuilder.start().add("$regex", "3049643224966").get();
		DBObject query =BasicDBObjectBuilder.start().add("c", gt).get();
		DBObject orderBy = BasicDBObjectBuilder.start()
				.add("nid", Integer.valueOf(-1)).get();
		
		DBCursor cursor = collection.find(query).sort(orderBy).limit(5);
		
		List<KuroroMessage> list = new ArrayList<KuroroMessage>();
		try {
			if(cursor.hasNext()){
			while (cursor.hasNext()) {
				DBObject result = cursor.next();
				KuroroMessage kuroroMessage = new KuroroMessage();
				try {
					convert(result, kuroroMessage);
					System.out.println(kuroroMessage.getContent()+"...."+kuroroMessage);
					list.add(kuroroMessage);
				} catch (RuntimeException e) {
					e.printStackTrace();
				}
			}
		}else{
			System.out.println("no MESSAGE return .....");
		}
		}
		finally {
			cursor.close();
		}
	}
	
	public void updateTest(){
		DBCollection collection = connectTest().getDB("admin").getCollection("zz_1");
		DBObject query = BasicDBObjectBuilder.start().add("product_id", 3333).get();
		DBObject update = BasicDBObjectBuilder.start().add("category_tab_id", 1).get();
		DBObject updateOps = BasicDBObjectBuilder.start().add("$inc", update).get();
		WriteResult result = collection.update(query, updateOps);
		System.out.println("result>>>"+result);
	}
	
	public MongoClient connectTest(){
		List<ServerAddress> replicaSetSeeds = new ArrayList<ServerAddress>();
		//replicaSetSeeds.add(new ServerAddress("10.161.144.68", 27016));
		replicaSetSeeds.add(new ServerAddress("10.161.144.67", 27017));
		char [] pwd_char = "kuroro123".toCharArray();
		MongoCredential credent = MongoCredential.createCredential("kuroro", "admin", pwd_char);
		List<MongoCredential> credentList = new ArrayList<MongoCredential>();
		credentList.add(credent);
		MongoClient mongoClient = new MongoClient(replicaSetSeeds, credentList, getMongoClientOptions());
		System.out.println(mongoClient.getServerAddressList()+"...."+mongoClient.getReadPreference().isSlaveOk());
		return mongoClient;
	}
	
	
	private MongoClientOptions getMongoClientOptions() {
		MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
		 
		builder.connectionsPerHost(30);
		builder.socketKeepAlive(true);
		builder.socketTimeout(15000);
		builder.threadsAllowedToBlockForConnectionMultiplier(10);
		builder.connectTimeout(5000);
		builder.maxWaitTime(5000);
		builder.serverSelectionTimeout(10000);
		MongoClientOptions options = builder.build();
		return options;
	}
	
	private void convert(DBObject result, KuroroMessage kuroroMessage) {
		BSONTimestamp timestamp = (BSONTimestamp) result.get("nid");
		kuroroMessage.setMessageId(BSONTimestampToLong(timestamp));
		kuroroMessage.setContent(result.get(CONTENT));// content
		kuroroMessage.setVersion((String) result.get(VERSION));// version
		kuroroMessage.setGeneratedTime((Date) result.get(GENERATED_TIME));// generatedTime
		Map<String, String> propertiesBasicDBObject = (Map<String, String>) result
				.get(PROPERTIES);
		if (propertiesBasicDBObject != null) {
			HashMap<String, String> properties = new HashMap<String, String>(
					propertiesBasicDBObject);
			kuroroMessage.setProperties(properties);// properties
		}
		Map<String, String> internalPropertiesBasicDBObject = (Map<String, String>) result
				.get(INTERNAL_PROPERTIES);
		if (internalPropertiesBasicDBObject != null) {
			HashMap<String, String> properties = new HashMap<String, String>(
					internalPropertiesBasicDBObject);
			kuroroMessage.setInternalProperties(properties);// properties
		}
		kuroroMessage.setSha1((String) result.get(SHA1));// sha1
		kuroroMessage.setType((String) result.get(TYPE));// type
		kuroroMessage.setSourceIp((String) result.get(SOURCE_IP));// sourceIp

		String protocolType = (String) result.get(PROTOCOLTYPE);
		if (protocolType != null)
			kuroroMessage.setProtocolType(protocolType);// protocolType
	}

	@Test
	public void queryMessageZone(){
		String zone = "ze";
		DBCollection collection = connectTest().getDB("msg_zfx_test_2").getCollection("c");
		long minMessageId = 6397628682478813200L, maxMessageId = 6397628686773780700L;
		String NID = "nid";
		HashSet<String> messageFilter = new HashSet<String>();
		messageFilter.add("test");

		DBObject gt = BasicDBObjectBuilder.start().add(QueryOperators.GT, MongoUtil.longToBSONTimestamp(minMessageId))
				.add(QueryOperators.LTE, MongoUtil.longToBSONTimestamp(maxMessageId)).get();

		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add(NID, gt);

		if (messageFilter != null) {
			Set<String> words = messageFilter;
			if (words != null && !words.isEmpty()) {
				// 添加过滤逻辑
				builder.add(TYPE, new BasicDBObject(QueryOperators.IN, words));
			}
		}

		HashSet<String> zoneFilter = new HashSet<String>();
		zoneFilter.add("ZONE_NH");

		if (zoneFilter != null) {
			Set<String> words = zoneFilter;
			if (words != null && !words.isEmpty()) {
				// 添加过滤逻辑
				builder.add(zone, new BasicDBObject(QueryOperators.IN, words));
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
					System.out.println("result====" + result);
				} catch (RuntimeException e) {
					System.out.println("Error when convert resultset to kuroroMessage." + e);
				}
			}
		} finally {
			cursor.close();
		}
	}

	@Test
	public void messageIdIsExist(){
		long offset = 6378623628287672381L;

		DBObject query = BasicDBObjectBuilder.start().add("nid",
				MongoUtil.longToBSONTimestamp(offset)).get();

		String NID ="nid";
		DBCollection collection = connectTest().getDB("msg_zfx_test_2").getCollection("c");


		DBObject fields = BasicDBObjectBuilder.start().add(NID, 1).get();
		DBObject orderBy = BasicDBObjectBuilder.start().add(NID, Integer.valueOf(1)).get();
		DBCursor cursor = collection.find(null, fields).sort(orderBy).limit(1);
		try {
			if (cursor.hasNext()) {
				BSONTimestamp timestamp = (BSONTimestamp) cursor.next().get(NID);
				Long messageId = BSONTimestampToLong(timestamp);
				System.out.println("messageId====" + messageId);
			}
		} finally {
			cursor.close();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new MongoDBQueryTest();
	}

}

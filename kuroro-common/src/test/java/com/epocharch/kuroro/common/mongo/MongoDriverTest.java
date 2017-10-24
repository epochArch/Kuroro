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

import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.dao.impl.mongodb.CompensationDaoImpl;
import com.epocharch.kuroro.common.inner.dao.impl.mongodb.NewMongoClient;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import junit.framework.Assert;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;
import org.junit.Test;
import org.kubek2k.springockito.annotations.SpringockitoContextLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(loader = SpringockitoContextLoader.class, locations = "classpath:applicationContext-test.xml")
public class MongoDriverTest extends AbstractJUnit4SpringContextTests{
	
	
	public static final String COL_TOPIC = "t";
	public static final String COL_CONSUMER = "c";
	public static final String COL_UNTIL_ID = "i";
	
	protected static final String TOPIC_NAME = "default";
	protected static final String CONSUMER_ID = "consumer1";
	protected static final String IP = "10.161.144.67";
	
	@Autowired
	private MessageDAO messageDAO;
	
	@Autowired
	private NewMongoClient newMongoClient;
		
	@Autowired
	private AckDAO ackDAO;
	
	@Test
	public void test() {
		
	}
	
	/*
	 * mongodb new auth 
	 * Exception : 
	 * */
	@Test
	public void autoenticationMongo(){

		String[] mongoURL = {"10.161.144.67"};
		char[] pwd_char = "kuroro123".toCharArray();
		MongoCredential credent = MongoCredential.createCredential("kuroro", "admin", pwd_char);
		List<MongoCredential> credentList = new ArrayList<MongoCredential>();
		credentList.add(credent);
		List<ServerAddress> serverList = new ArrayList<ServerAddress>();
		
		for(int i = 0; i< mongoURL.length; i++){
			serverList.add(new ServerAddress(mongoURL[i],27017));
			System.out.println("mongoclient==="+newMongoClient);
			MongoClient mm = new MongoClient(serverList, credentList, newMongoClient.getMongoClientOptions());
			serverList.clear();
			getMongoCollection(mm);
		}
	}

	//get mongodb collections replace db.collectionExists 
	public void getMongoCollection(MongoClient mongoClient){
		boolean isCollectionExists = false;
		/*MongoIterable<String> mongoIterable = mongoClient.listDatabaseNames();
		MongoCursor<String> cursor = mongoIterable.iterator();
		while(cursor.hasNext()){
			String dbName = cursor.next();
			// && dbName.equals("msg_zzz_1")
			if(dbName != null ){
				isCollectionExists = true;
				System.out.println("dbName......."+dbName);
			}
		}*/
		
		MongoDatabase db = mongoClient.getDatabase("compensation");
		MongoIterable<String> mongoCollection = db.listCollectionNames();
		MongoCursor<String> cursor = mongoCollection.iterator();
		while(cursor.hasNext()){
			String dbName = cursor.next();
			System.out.println("c==="+dbName);
			if(dbName.equals("c")){
				isCollectionExists = true;
				System.out.println("c exists==="+mongoClient);
			}
		}
		
		//new create index replace ensureIndex method
		MongoCollection<Document> collection = null;
		if(!isCollectionExists){
			db.createCollection("c", new CreateCollectionOptions());
			collection = db.getCollection("c");
			Bson indexBson = new Document(CompensationDaoImpl.COL_TOPIC, 1);
			collection.createIndex(indexBson);
		}else{
			collection = db.getCollection("c");
		}
		
	}


	@Test
	public void testGetMaxMessageId() {
//		// 添加一条记录
		int time = (int) (System.currentTimeMillis() / 1000);
		int inc = 1;
		BSONTimestamp timestamp = new BSONTimestamp(time, inc);
		Long expectedMessageId = MongoUtil.BSONTimestampToLong(timestamp);
		System.out.println(MongoUtil.BSONTimestampToLong(timestamp));
		ackDAO.add("zzz_1", "fuck2",
				MongoUtil.BSONTimestampToLong(timestamp), IP);
		ackDAO.add("zzz_1", "fuck2",
				MongoUtil.BSONTimestampToLong(timestamp), IP);
		// 测试
		Long maxMessageId = ackDAO.getMaxMessageID(TOPIC_NAME, CONSUMER_ID);
		System.out.println(expectedMessageId);
		System.out.println(maxMessageId);
		Assert.assertEquals(expectedMessageId, maxMessageId);
	}
	
	
	@Test
	public void testSaveMessage() {

		// 插入消息
		for (int i = 0; i < 2; i++) {
			KuroroMessage expectedMessage = createMessage();
			// expectedMessage.setContent("content in testSaveMessage");
			messageDAO.saveMessage("zzz_1", expectedMessage, 1);
			// 查询消息是否正确
			 KuroroMessage actualMessage =
			 messageDAO.getMaxMessage("zzz_1");
			 Assert.assertTrue(expectedMessage.equalsWithoutMessageId(actualMessage));
		}
	}
	
	private static KuroroMessage createMessage() {
		KuroroMessage message = new KuroroMessage();
		Student s = new Student();
		s.setName("yyy");
		s.setNo("mm");
		message.setContent(s);
		message.setGeneratedTime(new Date());
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("property-key", "property-value");
		message.setProperties(map);
		message.setSha1("sha-1 string");
		message.setVersion("0.6.0");
		message.setType("feed");
		message.setSourceIp("localhost");
		return message;
	}
	
}

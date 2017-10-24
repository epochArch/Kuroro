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

import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.dao.impl.mongodb.NewMongoClient;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.bson.types.BSONTimestamp;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class MessageDAOImplTest extends AbstractDAOImplTest {

	@Autowired
	private MessageDAO messageDAO;

	@Autowired
	private NewMongoClient newMongoClient;

	@Test
	public void testMongoNewClientTest() {
		Long currentMessageId = 5858184311974396553L;// MongoUtil.getLongByCurTime();

		Long maxMessageId = messageDAO.getMessageIDGreaterThan(
				"topic_register", currentMessageId, 100, 0);

		while (true) {
			try {
				Map<String, List<MongoClient>> m = newMongoClient
						.getTopicNameToMongoMap();
				Iterator<String> keys = m.keySet().iterator();
				while (keys.hasNext()) {
					String key = keys.next();
					List<MongoClient> set = m.get(key);
					System.out.println("============" + key + "=============");
					int i = 0;
					for (Mongo mm : set) {
						i++;
						System.out.println("=========" + i + "====");
						for (ServerAddress s : mm.getAllAddress()) {
							System.out.println(s.getHost() + ":" + s.getPort());
						}

					}
				}
				System.out.println("=============End=============");
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Test
	public void testMinMaxMessage() {
		Long currentMessageId = 5868424196907859969L;// MongoUtil.getLongByCurTime();
		// 727f3b8a5868420541890691073
		Long maxMessageId = 5868425360843997223L;// messageDAO.getMessageIDGreaterThan("topic_register",
													// currentMessageId, 100,
													// 0);
		System.out.println(currentMessageId + "---->" + maxMessageId + "--->");
		List<KuroroMessage> ls = messageDAO.getMessagesGreaterThan(
				"BsPriceChangeQueue", currentMessageId, maxMessageId,null, 0, null);
		System.out.println(ls.size());

		// 5841354998021095425---->5841355539186974871
	}

	@Test
	public void testListBw() {
		Long ls = messageDAO.getMaxMessageId("topic_register");
		Long min = null;
		System.out.println(ls);
		// Long next=messageDAO.getMessageIDGreaterThan("topic_register", null,
		// 100);
		// System.out.println(next);
		// List<KuroroMessage> l=
		// messageDAO.getMessagesGreaterThan("topic_register", min, ls,0);
		// System.out.println(l.size());

	}

	@Test
	public void testTime() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time = "2013-03-19 17:00:00";

		try {
			Date date = format.parse(time);
			System.out.println(date.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSaveMessage() {

		// 插入消息
		for (int i = 0; i < 2; i++) {
			KuroroMessage expectedMessage = createMessage();
			// expectedMessage.setContent("content in testSaveMessage");
			messageDAO.saveMessage("ytest", expectedMessage, 1);
			// 查询消息是否正确
			// KuroroMessage actualMessage =
			// messageDAO.getMaxMessage(TOPIC_NAME);
			// Assert.assertTrue(expectedMessage.equalsWithoutMessageId(actualMessage));
		}
	}

	@Test
	public void testMaxAfter() {
		BSONTimestamp bs = new BSONTimestamp(1399972862, 0);
		BSONTimestamp bs2 = MongoUtil.longToBSONTimestamp(messageDAO
				.getMinIdAfter("archtest", bs));
		System.out.println(bs2.getTime() + "," + bs2.getInc());
		BSONTimestamp bs3 = MongoUtil.longToBSONTimestamp(messageDAO
				.getMinIdAfter("archtest", bs2));
		System.out.println(bs3.getTime() + "," + bs3.getInc());
	}

	@Test
	public void testCount() {
		BSONTimestamp b1 = new BSONTimestamp(1398765317, 11);
		BSONTimestamp b2 = new BSONTimestamp(1398765317, 23);
		System.out.println(System.currentTimeMillis());
		System.out.println(messageDAO.countBetween("archtest",
				MongoUtil.BSONTimestampToLong(b1),
				MongoUtil.BSONTimestampToLong(b2)));
		System.out.println(System.currentTimeMillis());
	}

	private static KuroroMessage createMessage() {

		KuroroMessage message = new KuroroMessage();
		// message.setProtocolType(ProtocolType.HESSIAN.toString());
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

	@Test
	public void testGetMessage() {
		// 插入消息
		KuroroMessage expectedMessage = createMessage();
		expectedMessage.setContent("content in testGetMessage");
		messageDAO.saveMessage(TOPIC_NAME, expectedMessage, 1);
		// 查询消息是否正确
		Long maxMessageId = messageDAO.getMaxMessageId(TOPIC_NAME);
		KuroroMessage actualMessage = messageDAO.getMessage(TOPIC_NAME,
				maxMessageId);
		Assert.assertTrue(expectedMessage.equalsWithoutMessageId(actualMessage));
		System.out.println(actualMessage);
	}

	@Test
	public void testGetMessagesGreaterThan() {
		// 插入1条消息
		KuroroMessage message = createMessage();
		messageDAO.saveMessage(TOPIC_NAME, message, 1);
		// 获取消息id
		Long maxMessageId = messageDAO.getMaxMessageId(TOPIC_NAME);
		// 再插入2条消息
		KuroroMessage expectedMessage1 = createMessage();
		messageDAO.saveMessage(TOPIC_NAME, expectedMessage1, 1);
		KuroroMessage expectedMessage2 = createMessage();
		messageDAO.saveMessage(TOPIC_NAME, expectedMessage2, 1);
		KuroroMessage expectedMessage3 = createMessage();
		messageDAO.saveMessage(TOPIC_NAME, expectedMessage3, 1);

		// 查询messageId比指定id大的按messageId升序排序的2条消息
		List<KuroroMessage> messagesGreaterThan = messageDAO
				.getMessagesGreaterThan(TOPIC_NAME, maxMessageId, 5);
		System.out.println(messagesGreaterThan.size());
		Assert.assertNotNull(messagesGreaterThan);
		Assert.assertEquals(2, messagesGreaterThan.size());
		Assert.assertTrue(expectedMessage1
				.equalsWithoutMessageId(messagesGreaterThan.get(0)));
		Assert.assertTrue(expectedMessage2
				.equalsWithoutMessageId(messagesGreaterThan.get(1)));

	}

	@Test
	public void showMessage() {
		KuroroMessage msg = messageDAO.getMessage("inshopProductPunish",
				5891493780514668545L);
		System.out.println(msg.getContent());
	}

}

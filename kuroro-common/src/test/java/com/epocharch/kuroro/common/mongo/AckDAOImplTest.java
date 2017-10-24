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

import com.epocharch.kuroro.common.inner.util.MongoUtil;
import java.io.File;
import java.util.Scanner;
import org.junit.Test;

public class AckDAOImplTest {//extends AbstractDAOImplTest {
//	@Autowired
//	private AckDAO ackDAO;
//	@Autowired
//	private MessageDAO messageDAO;

	@Test
	public void testAdd() throws Exception {
        Scanner scanner = new Scanner(new File("/Users/bill/Downloads/tmp.txt"));
String line = null;
        while((line= scanner.nextLine()) != null) {
            //params=>dmp_user_creative_product,6130000182114257332,264,0,result=>6130000852129153343
            String[] arr = line.split(",");

            Long l1 = Long.parseLong(arr[1]);
            Long l2 = Long.parseLong(arr[4].substring(8));
            int t1 = MongoUtil.longToBSONTimestamp(l1).getTime();
            int inc1 = MongoUtil.longToBSONTimestamp(l1).getInc();

            int t2 = MongoUtil.longToBSONTimestamp(l2).getTime();
            int inc2 = MongoUtil.longToBSONTimestamp(l2).getInc();

            System.out.println("use msg_dmp_user_creative_product;");
            System.out.println("db.c.find({_id:{$gt:new Timestamp(" + t1 + "000," + inc1 + "),$lte:new Timestamp(" + t2 + "000," + inc2 + ")}}).count();");
            System.out.println("use ack_dmp_user_creative_product#dmp_user_creative_product;");
            System.out.println("db.c.find({_id:{$gt:new Timestamp(" + t1 + "000," + inc1 + "),$lte:new Timestamp(" + t2 + "000," + inc2 + ")}}).count();");
        }
	}

//	@Test
//	public void testAckGetMessage() {
//		Long currentMessageId = ackDAO.getMaxMessageID("BsPriceChangeQueue",
//				"consumer-poolName", 0);
//		System.out.println(currentMessageId);
//		if (currentMessageId == null) {
//			currentMessageId = messageDAO.getMaxMessageId("BsPriceChangeQueue");
//			System.out.println(currentMessageId);
//			if (currentMessageId == null) {
//				currentMessageId = MongoUtil.getLongByCurTime();
//
//			}
//		}
//	}
//
//	@Test
//	public void testGetMaxMessageId() {
//		// 添加一条记录
//		int time = (int) (System.currentTimeMillis() / 1000);
//		int inc = 1;
//		BSONTimestamp timestamp = new BSONTimestamp(time, inc);
//		Long expectedMessageId = MongoUtil.BSONTimestampToLong(timestamp);
//		System.out.println(MongoUtil.BSONTimestampToLong(timestamp));
//		ackDAO.add("inshop_store_change", "inshop_store_consumer",
//				MongoUtil.BSONTimestampToLong(timestamp), IP);
//		// 测试
//		Long maxMessageId = ackDAO.getMaxMessageID(TOPIC_NAME, CONSUMER_ID);
//		System.out.println(expectedMessageId);
//		System.out.println(maxMessageId);
//		Assert.assertEquals(expectedMessageId, maxMessageId);
//	}
//
//	public Long formatDateTime(String date, String pattern) {
//		SimpleDateFormat f = new SimpleDateFormat(pattern);
//		Date dd = null;
//		try {
//			dd = f.parse(date);
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return dd.getTime();
//	}
}

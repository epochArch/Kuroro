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

package com.epocharch.kuroro.broker;

import com.epocharch.kuroro.broker.leaderserver.LeaderClient;
import com.epocharch.kuroro.broker.leaderserver.LeaderClientManagerFactory;
import com.epocharch.kuroro.broker.leaderserver.MessageIDPair;
import com.epocharch.kuroro.broker.leaderserver.WrapTopicConsumerIndex;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.epocharch.kuroro.common.jmx.support.JMXServiceURLBuilder;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.kuroro.common.netty.component.RouteManager;
import java.util.Date;
import java.util.List;
import org.bson.types.BSONTimestamp;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class LeaderRoutingTest extends AbstractDAOImplTest {

	private List<String> hosts = null;

	@Autowired
	private MessageDAO messageDAO;
	private RouteManager routerManager = new RouteManager(
			"/EpochArch/IDC1/Kuroro/Ephe/BrokerGroup/Default/Leader");

	@Test
	public void getLeader() {
		// topicname#consumerId#index
		WrapTopicConsumerIndex t = new WrapTopicConsumerIndex();
		t.setConsumerId("consumer1");
		t.setIndex(0);
		t.setTopicName("topic_test");
		LeaderClient lead = LeaderClientManagerFactory.getClientManager()
				.getLeaderClient(t);
		System.out.println(lead.getAddress() + "--" + lead.isConnected());
		MessageIDPair pair = lead.sendTopicConsumer(t);
		System.out.println(pair.getMinMessageId() + "===="
				+ pair.getMaxMessageId());
	}

	@Test
	public void Contrains() {
		WrapTopicConsumerIndex t = new WrapTopicConsumerIndex();
		t.setConsumerId("consumerTest");
		t.setIndex(0);
		t.setTopicName("topic_test");
		if (t.getTopicConsumerIndex().contains(t.getTopicName())) {
			System.out.println(t.getTopicConsumerIndex());
		}
	}

	@Test
	public void SelectHitest() {
		WrapTopicConsumerIndex topicConsumerIndex = new WrapTopicConsumerIndex();
		topicConsumerIndex.setTopicName("csp_sys_log");
		topicConsumerIndex.setIndex(0);
		topicConsumerIndex.setConsumerId("csp_sys_log_Consumer");
		topicConsumerIndex.setCommand(null);

		HostInfo hi = LeaderClientManagerFactory.getClientManager().select(
				topicConsumerIndex);
		System.out.println(hi.getConnect());

	}

	@Test
	public void LeadClientTest() {
		WrapTopicConsumerIndex topicConsumerIndex = new WrapTopicConsumerIndex();
		topicConsumerIndex.setTopicName("BsPriceChangeQueue");
		topicConsumerIndex.setIndex(0);
		topicConsumerIndex.setConsumerId("consumer-poolName");
		topicConsumerIndex.setCommand(null);
		LeaderClient client = LeaderClientManagerFactory.getClientManager()
				.getLeaderClient(topicConsumerIndex);
		if (client != null) {
			MessageIDPair pair = client.sendTopicConsumer(topicConsumerIndex);
			// System.out.print(pair);
			if (pair != null) {
				System.out.println(pair.getMinMessageId() + "-->"
						+ pair.getMaxMessageId());
			}
		}
	}

	@Test
	public void RandomTest() {
		System.out.println(convertToDate(5885923014658228225L));
	}

	private Date convertToDate(Long messageId) {
		BSONTimestamp timeStamp = MongoUtil.longToBSONTimestamp(messageId);
		Long time = timeStamp.getTime() * 1000L;
		Date _time = new Date(time);
		return _time;
	}


	@Test
	public void JmxBuilderTest() {
		JMXServiceURLBuilder x = new JMXServiceURLBuilder(3997, "jmxrmi");
		System.out.println(x.getJMXServiceURLToString());
	}
}

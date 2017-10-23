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

package com.epocharch.kuroro.consumerclient;

import com.epocharch.kuroro.common.consumer.ConsumerType;
import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.exceptions.SendFailedException;
import com.epocharch.kuroro.common.message.Destination;
import com.epocharch.kuroro.common.message.Message;
import com.epocharch.kuroro.consumer.BackoutMessageException;
import com.epocharch.kuroro.consumer.Consumer;
import com.epocharch.kuroro.consumer.ConsumerConfig;
import com.epocharch.kuroro.consumer.MessageListener;
import com.epocharch.kuroro.consumer.NeedResendException;
import com.epocharch.kuroro.consumer.impl.ConsumerFactoryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhoufeiqiang on 21/10/2017.
 */
public class KuroroConsumerClientTest {

  private Consumer consumer = null;

  @Before
  public void init() {
    ConsumerConfig config = new ConsumerConfig();
    // config.setConsumerType(ConsumerType.AUTO_ACKNOWLEDGE);
    config.setConsumerType(ConsumerType.CLIENT_ACKNOWLEDGE);
    config.setThreadPoolSize(3);
    //config.setIdcProduct(true);
    //Set<String> messageFilter = new HashSet<String>();
    //messageFilter.add("test");
    //config.setConsumeLocalZoneFilter(MessageFilter.createInSetMessageFilter(messageFilter));
    //config.setMessageFilter(MessageFilter.createInSetMessageFilter(messageFilter));
    //config.setIsConsumeLocalZone(true);

    consumer = ConsumerFactoryImpl.getInstance().createConsumer(
        Destination.topic("kuroroTest"), "kuroroConsumer", config);
    MessageFilter messageFilter = config.getConsumeLocalZoneFilter();
  }

  @Test
  public void consume() throws SendFailedException {

    consumer.setListener(new MessageListener() {
      @Override
      public void onMessage(Message msg) throws BackoutMessageException,
          NeedResendException {
        System.out.println("message===" + msg.getContent());
      }
    });

    consumer.start();
  }

  @After
  public void after() throws InterruptedException {
    Thread.sleep(1888 * 1888*10);
  }
}

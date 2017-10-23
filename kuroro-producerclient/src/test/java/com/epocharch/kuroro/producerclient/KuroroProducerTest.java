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

package com.epocharch.kuroro.producerclient;


import com.epocharch.kuroro.common.ProtocolType;
import com.epocharch.kuroro.common.inner.exceptions.SendFailedException;
import com.epocharch.kuroro.common.message.TransferDestination;
import com.epocharch.kuroro.producer.Producer;
import com.epocharch.kuroro.producer.ProducerConfig;
import com.epocharch.kuroro.producer.SendMode;
import com.epocharch.kuroro.producer.impl.ProducerFactoryImpl;
import org.junit.Test;

/**
 * Created by zhoufeiqiang on 10/08/2017.
 */
public class KuroroProducerTest {

  @Test
  public void kuroroSyncProducer() throws SendFailedException {
    ProducerConfig config = new ProducerConfig();
    config.setMode(SendMode.SYNC_MODE);
    config.setZipped(true);//是否开启对消息体的压缩，默认不压缩消息体
    Producer p = ProducerFactoryImpl.getInstance().createProducer(
        TransferDestination.topic("kuroroTest"), config);
    String  content = "测试消息内容";
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("fuck "+i+" -> "
          + p.sendMessage(i, "test", ProtocolType.JSON));
    }
  }

  @Test
  public void kuroroAsyncProducer() throws SendFailedException {
    ProducerConfig config = new ProducerConfig();
    config.setMode(SendMode.ASYNC_MODE);
    config.setZipped(true);//是否开启对消息体的压缩，默认不压缩消息体
    config.setThreadPoolSize(10);
    config.setAsyncRetryTimes(5);

    Producer p = ProducerFactoryImpl.getInstance().createProducer(
        TransferDestination.topic("zfx_test_2"), config);

    String  content = "测试消息内容";
    //消息序列化方式主要分为2种：JSON 和 Hessian,默认 Hessian
    p.sendMessage(content, ProtocolType.JSON);
  }



  public static void main(String args[]) {

  }
}

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

package com.epocharch.kuroro.monitor;

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.inner.config.impl.TopicConfigDataMeta;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Created by zhoufeiqiang on 20/10/2017.
 */
public class TopicTest {

  @Test
  public void createAndEditTopic(){
    TopicConfigDataMeta cd = new TopicConfigDataMeta();
    cd.setTopicName("kuroroTest");
    cd.setPoolName("test");
    cd.setOwnerEmail("test@xxx.com");
    cd.setOwnerName("zzz");
    cd.setOwnerPhone("111111111");
    cd.setType("mongo");
    cd.setBrokerGroup("Default");
    cd.setAckCappedMaxDocNum(10);
    cd.setAckCappedSize(10);
    cd.setMessageCappedMaxDocNum(100);
    cd.setMessageCappedSize(100);
    cd.setLevel(1);
    cd.setSendFlag(true);
    cd.setMongoGroup("test_1");
    cd.setTimeStamp(System.currentTimeMillis());
    cd.setFlowSize(5000);
    List<String> mongo = new ArrayList<String>();
    mongo.add("mongodb://10.161.144.67:27017");
    cd.setReplicationSetList(mongo);

    ZkClient zkClient = KuroroZkUtil.initIDCZk();

    String topicRootPath = "/EpochArch/IDC1/Kuroro/Pers/Topic" + Constants.SEPARATOR + cd.getTopicName();
    if (zkClient.exists(topicRootPath)) {
      zkClient.writeData(topicRootPath, cd);
    }else {
      zkClient.createPersistent(topicRootPath, true);
      zkClient.writeData(topicRootPath, cd);
    }
  }

  @Test
  public void deleteTopic() {
    String topicName = "test";
    String topicPath = "/EpochArch/IDC1/Kuroro/Pers/Topic/" + topicName;
    ZkClient zkClient = KuroroZkUtil.initIDCZk();

    if (zkClient.exists(topicPath)) {
      zkClient.delete(topicPath);
    }else {
      System.out.println("topic path is not exist! please check topic path!");
    }
  }

}

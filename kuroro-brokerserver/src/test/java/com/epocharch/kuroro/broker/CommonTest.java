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

import com.epocharch.kuroro.broker.leaderserver.WrapTopicConsumerIndex;
import com.epocharch.kuroro.common.protocol.json.JsonBinder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * @author bill
 * @date 9/5/14
 */
public class CommonTest {

    static class WrapTopicConsumerIndex2 extends WrapTopicConsumerIndex {

    }

    @Test
    public void testLeaderProtocol() throws Exception {
        WrapTopicConsumerIndex obj = new WrapTopicConsumerIndex();
        obj.setCommand("sx");
        obj.setConsumerId("sx");
        obj.setIndex(2);
        obj.setRequestTime(2L);
        obj.setSquence(0L);
        obj.setTopicName("sb");

        JsonBinder jsonBinder = JsonBinder.getNonEmptyBinder();
        String json = jsonBinder.toJson(obj);
        System.out.println(json);
        byte[] jsonBytes = json.getBytes(Charset.forName("UTF-8"));

        System.out.println(Arrays.toString(jsonBytes));


        String j = new String(jsonBytes, Charset.forName("UTF-8"));
        JsonBinder binder = JsonBinder.getNonEmptyBinder();
        WrapTopicConsumerIndex2 obj2 = binder.fromJson(j, WrapTopicConsumerIndex2.class);
        System.out.println(obj2);
        Set<String> s = new HashSet<String>();
        s.add("1");
        s.add("2");

        JsonBinder j2 = JsonBinder.getNonEmptyBinder();
        String json2 = j2.toJson(obj2);

        System.out.println(json2);

        WrapTopicConsumerIndex obj3 = j2.fromJson(json2, WrapTopicConsumerIndex.class);
        System.out.println(obj3);

    }
}

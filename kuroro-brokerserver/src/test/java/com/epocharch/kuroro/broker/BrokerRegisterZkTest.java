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


import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.common.netty.component.HostInfo;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Created by zhoufeiqiang on 17/02/2017.
 */
public class BrokerRegisterZkTest {

    private ZkClient zkClient = KuroroZkUtil.initIDCZk();

    @Test
    public void registerBrokerIPToIdcZk(){

        String idcBrokerServerPath = "/EpochArch/IDC1/Kuroro/Ephe/BrokerGroup/Default";
        String leaderPath = idcBrokerServerPath + "/Leader";
        String producerPath = idcBrokerServerPath + "/Producer";
        String consumerPath = idcBrokerServerPath + "/Consumer";

        List<String> brokerIP = new ArrayList<String>();
        brokerIP.add("10.161.144.67");
        brokerIP.add("10.161.144.68");

        for(int i = 0; i < brokerIP.size(); i++){
            String ip = brokerIP.get(i);
            HostInfo leaderHI = new HostInfo(ip,
                    Constants.KURORO_LEADER_PORT, 1);
            HostInfo producerHi = new HostInfo(ip,
                      Constants.KURORO_PRODUCER_PORT, 1);
            HostInfo consumerHi = new HostInfo(ip,
                    Constants.KURORO_CONSUMER_PORT, 1);

            String _leaderPath = leaderPath + "/" + ip;
            String _producerPath = producerPath + "/" + ip;
            String _consumerPath = consumerPath + "/" + ip;

            if(zkClient.exists(leaderPath)) {
                zkClient.createEphemeral(_leaderPath, leaderHI);
                System.out.println("------leader started-----" + leaderPath);
            }else {
                zkClient.createPersistent(leaderPath, true);
                zkClient.createEphemeral(_leaderPath, leaderHI);
            }
            if(zkClient.exists(producerPath)) {
                zkClient.createEphemeral(_producerPath, producerHi);
                System.out.println("-----producer started----" + producerPath);
            }else {
                zkClient.createPersistent(producerPath);
                zkClient.createEphemeral(_producerPath, producerHi);
            }

            if (zkClient.exists(consumerPath)) {
                zkClient.createEphemeral(_consumerPath, consumerHi);
                System.out.println("-----consumer started----" + consumerPath);
            }else {
                zkClient.createPersistent(consumerPath, true);
                zkClient.createEphemeral(_consumerPath, consumerHi);
            }
        }

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void registerBrokerIPToZoneZk() {
        List<String> zonesList = new ArrayList<String>();
        zonesList.add("idc_1");
        zonesList.add("idc_2");
        zonesList.add("idc_3");

        String zoneBrokerServer = "/EpochArch/IDC1/Kuroro/";

        List<String> brokerIP = new ArrayList<String>();
        brokerIP.add("10.161.144.67");
        brokerIP.add("10.161.144.68");

        for (String zone : zonesList) {
            String zoneBrokerPath = zoneBrokerServer + zone + "/Ephe/BrokerGroup/Default";
            String leaderPath = zoneBrokerPath + "/Leader";
            String producerPath = zoneBrokerPath + "/Producer";
            String consumerPath = zoneBrokerPath + "/Consumer";

            for (int i = 0; i < brokerIP.size(); i++) {
                String ip = brokerIP.get(i);
                HostInfo leaderHI = new HostInfo(ip,
                    Constants.KURORO_LEADER_PORT, 1);
                HostInfo producerHi = new HostInfo(ip,
                    Constants.KURORO_PRODUCER_PORT, 1);
                HostInfo consumerHi = new HostInfo(ip,
                    Constants.KURORO_CONSUMER_PORT, 1);

                String _leaderPath = leaderPath + "/" + ip;
                String _producerPath = producerPath + "/" + ip;
                String _consumerPath = consumerPath + "/" + ip;
                if (zkClient.exists(leaderPath)) {
                    zkClient.createEphemeral(_leaderPath, leaderHI);
                    System.out.println("------leader started-----" + leaderPath);
                }else {
                    zkClient.createPersistent(leaderPath, true);
                    zkClient.createEphemeral(_leaderPath, leaderHI);
                }

                if (zkClient.exists(producerPath)) {
                    zkClient.createEphemeral(_producerPath, producerHi);
                    System.out.println("-----producer started----" + producerPath);
                }else {
                    zkClient.createPersistent(producerPath);
                    zkClient.createEphemeral(_producerPath, producerHi);
                }

                if (zkClient.exists(consumerPath)) {
                    zkClient.createEphemeral(_consumerPath, consumerHi);
                    System.out.println("-----consumer started----" + consumerPath);
                }else {
                    zkClient.createPersistent(consumerPath, true);
                    zkClient.createEphemeral(_consumerPath, consumerHi);
                }
            }
        }

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}

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

package com.epocharch.kuroro.common.constants;

public interface Constants {

  //Zone Design add
  public static final String ZONE_ZK_PARENT_ROOT = "/EpochArch";
  public static final String PRODUCTION = "/Kuroro";
  public static final String ZONES = "/zones";
  public static final String ZONE_DATA_EPHE = "/Ephe";
  public static final String ZONE_DATA_PERS = "/Pers";
  public static final String ZONE_MONGO = ZONE_DATA_PERS + "/Mongo";
  public static final String ZONE_TOPIC = ZONE_DATA_PERS + "/Topic";
  public static final String ZONE_ZK_TOPIC = "/Topic";
  public static final String ZONE_BROKER = ZONE_DATA_EPHE + "/BrokerGroup";
  public static final String ZONE_BROKER_LEADER = "/Leader";
  public static final String ZONE_BROKER_PRODUCER = "/Producer";
  public static final String ZONE_BROKER_CONSUMER = "/Consumer";
  public static final String ZONE_MONITOR = ZONE_DATA_EPHE + "/Monitor";
  public static final String IDC_MONGO = PRODUCTION + ZONE_MONGO;
  public static final String IDC_JMX_URL = ZONE_DATA_PERS + "/Jmx/Url";
  public static final String IDC_TRANS_CONFIG = ZONE_DATA_PERS + "/Trans-Config";
  public static final String IDC_TOPIC = PRODUCTION + ZONE_TOPIC;
  public static final String IDC_TRANSGROUP = ZONE_DATA_EPHE + "/TransferGroup";
  public static final String IDC_TRANS_CONSUMER = ZONE_DATA_EPHE + "/Trans-Consumer";
  public static final String IDC_OPEAPI = "/OpenApi";
  public static final String KURORO_CONSUMERS_PATH = ZONE_DATA_PERS + "/Consumers";
  public static final String KURORO_PRODUCER_PATH = ZONE_DATA_PERS + "/Producers";
  public static final String TRANSFER_BAND_WIDTH = "/TransferBandWidth";
  public static final String TRANSFER_BAND_WIDTH_PATH = ZONE_DATA_PERS + TRANSFER_BAND_WIDTH;
  public static final String DEV_PREFIX = "-dev";

  public static final String KURORO_ZK_PARENT_ROOT = (
      Boolean.parseBoolean(System.getProperty("kuroro.debug", "false")) ?
          (ZONE_ZK_PARENT_ROOT + DEV_PREFIX) :
          ZONE_ZK_PARENT_ROOT);
  public static final String KURORO_DEFAULT_BROKER_GROUPNAME = "Default";

  public static final char SEPARATOR = '/';
  public static final Integer KURORO_PRODUCER_PORT = 8082;
  public static final Integer KURORO_CONSUMER_PORT = 8081;
  public static final Integer KURORO_LEADER_PORT = 8083;
  public static final Integer KURORO_PRODUCER_HTTP_PORT = 8092;
  public static final Integer KURORO_CONSUMER_HTTP_PORT = 8091;
  public static final Integer KURORO_LEADER_HTTP_PORT = 8093;
  public static final int INTEGER_BARRIER = Integer.MAX_VALUE / 2;
  public static final int MIRROR_SEED = 30;
  public static final String PER_SEND_EMAIL_LIST = "xxx@xxx.com";
  public static final String SEND_EMAIL_LIST = "xxx@xxx.com";
  public static final long CONSUMER_CHECK_CYCLElIFE = 60L * 1000L;
  public static final String TEST_SEND_EMAIL_LIST = "";

  public static final String HASH_FUNCTION_MUR2 = "murmur2";

  public static final String ENV_TEST = "test";
  public static final String ENV_STAGING = "staging";
  public static final String ENV_PRODUCTION = "production";

  public static final String LEADER_CLOSE_MONITOR_PORT = "17557";
  public static final String CONSUMER_CLOSE_MONITOR_PORT = "17555";
  public static final String PRODUCER_CLOSE_MONITOR_PORT = "17556";
}

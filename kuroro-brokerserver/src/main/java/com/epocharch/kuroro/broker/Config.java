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

import com.epocharch.common.config.PropertiesContainer;
import com.epocharch.kuroro.broker.constants.BrokerInConstants;
import com.epocharch.kuroro.common.constants.Constants;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Config {

  private static final Logger LOG = LoggerFactory.getLogger(Config.class);
  private static Config instance;
  private int pullFailDelayBase = 500;
  private int pullFailDelayUpperBound = 3000;
  private long checkConnectedChannelInterval = 10000L;
  private long retryIntervalWhenMongoException = 2000L;
  private long waitAckTimeWhenCloseKuroro = 15000L;
  private long waitSlaveShutDown = 30000L;
  private long closeChannelMaxWaitingTime = 10000L;
  private int heartbeatCheckInterval = 2000;
  private int heartbeatMaxStopTime = 10000;
  private int heartbeatUpdateInterval = 2000;
  private int maxClientThreadCount = 100;
  private int consumerPort = Constants.KURORO_CONSUMER_PORT;
  private int producerPort = Constants.KURORO_PRODUCER_PORT;
  private int leaderPort = Constants.KURORO_LEADER_PORT;

  private int maxAckedMessageIdUpdateInterval = 1000;

  private Config() {
    PropertiesContainer.getInstance().loadProperties(BrokerInConstants.KURORO_BROKER_SERVER,
        BrokerInConstants.KURORO_BORKER_NAMESPACE);
  }

  public static Config getInstance() {
    if (instance == null) {
      instance = new Config();
    }
    return instance;
  }

  @SuppressWarnings("rawtypes")
  private void loadLocalConfig(Properties props) {
    Class clazz = this.getClass();
    for (String key : props.stringPropertyNames()) {
      Field field = null;
      try {
        field = clazz.getDeclaredField(key.trim());
      } catch (Exception e) {
        LOG.error("unknown property found: " + key);
        continue;
      }
      field.setAccessible(true);
      if (field.getType().equals(Integer.TYPE)) {
        try {
          field.set(this, Integer.parseInt(props.getProperty(key).trim()));
        } catch (Exception e) {
          LOG.error("can not parse property " + key, e);
          continue;
        }
      } else if (field.getType().equals(Long.TYPE)) {
        try {
          field.set(this, Long.parseLong(props.getProperty(key).trim()));
        } catch (Exception e) {
          LOG.error("can not set property " + key, e);
          continue;
        }
      } else if (field.getType().equals(String.class)) {
        try {
          field.set(this, props.getProperty(key).trim());
        } catch (Exception e) {
          LOG.error("can not set property " + key, e);
          continue;
        }
      } else {
        try {
          field.set(this, Boolean.parseBoolean(props.getProperty(key).trim()));
        } catch (Exception e) {
          LOG.error("can not set property " + key, e);
          continue;
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      Field[] fields = clazz.getDeclaredFields();
      for (int i = 0; i < fields.length; i++) {
        Field f = fields[i];
        f.setAccessible(true);
        if (!Modifier.isStatic(f.getModifiers())) {
          try {
            LOG.debug(f.getName() + "=" + f.get(this));
          } catch (Exception e) {
          }
        }
      }
    }
  }

  public int getPullFailDelayBase() {
    return pullFailDelayBase;
  }

  public int getPullFailDelayUpperBound() {
    return pullFailDelayUpperBound;
  }

  public long getCheckConnectedChannelInterval() {
    return checkConnectedChannelInterval;
  }

  public long getRetryIntervalWhenMongoException() {
    return retryIntervalWhenMongoException;
  }

  public long getWaitAckTimeWhenCloseKuroro() {
    return waitAckTimeWhenCloseKuroro;
  }

  public long getWaitSlaveShutDown() {
    return waitSlaveShutDown;
  }

  public long getCloseChannelMaxWaitingTime() {
    return closeChannelMaxWaitingTime;
  }

  public int getHeartbeatCheckInterval() {
    return heartbeatCheckInterval;
  }

  public int getHeartbeatMaxStopTime() {
    return heartbeatMaxStopTime;
  }

  public int getHeartbeatUpdateInterval() {
    return heartbeatUpdateInterval;
  }

  public int getMaxClientThreadCount() {
    return maxClientThreadCount;
  }

  public int getConsumerPort() {
    return consumerPort;
  }

  public int getProducerPort() {
    return producerPort;
  }

  public int getMaxAckedMessageIdUpdateInterval() {
    return maxAckedMessageIdUpdateInterval;
  }

  public int getLeaderPort() {
    return leaderPort;
  }

}

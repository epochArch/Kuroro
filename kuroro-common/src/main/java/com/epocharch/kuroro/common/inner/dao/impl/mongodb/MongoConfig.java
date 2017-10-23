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

package com.epocharch.kuroro.common.inner.dao.impl.mongodb;

import com.epocharch.common.config.PropertiesContainer;
import com.epocharch.kuroro.common.constants.InConstants;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoConfig {

  private static final Logger LOG = LoggerFactory.getLogger(MongoConfig.class);
  private boolean socketKeepAlive;
  private int socketTimeout;
  private int connectionsPerHost;
  private int threadsAllowedToBlockForConnectionMultiplier;
  private int w;
  private int wtimeout;
  private boolean fsync;
  private int connectTimeout;
  private int maxWaitTime;
  private boolean safe;
  private boolean j;
  private String userName;
  private String passWord;
  /*
   * mongodb-driver above 3.x , Sets the server selection timeout in
   * milliseconds, which defines how long the driver will wait for server
   * selection to succeed before throwing an exception. default is 30000ms
   */
  private int serverSelectionTimeout;
  private boolean slaveOk;

  public MongoConfig() {
    socketKeepAlive = true;
    socketTimeout = 5000;
    connectionsPerHost = 30;
    threadsAllowedToBlockForConnectionMultiplier = 50;
    w = 0;
    wtimeout = 5000;
    fsync = false;
    j = false;
    connectTimeout = 5000;
    maxWaitTime = 5000;
    safe = true;
    userName = "kuroro";
    passWord = "kuroro123";
    serverSelectionTimeout = 2000;
    setSlaveOk(true);

    PropertiesContainer.getInstance().
        loadProperties(InConstants.KURORO_MONGO_PROP, InConstants.KURORO_COMMON_NAMESPACE);
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

  public boolean isSocketKeepAlive() {
    return socketKeepAlive;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public int getConnectionsPerHost() {
    return connectionsPerHost;
  }

  public int getThreadsAllowedToBlockForConnectionMultiplier() {
    return threadsAllowedToBlockForConnectionMultiplier;
  }

  public int getW() {
    return w;
  }

  public int getWtimeout() {
    return wtimeout;
  }

  public boolean isFsync() {
    return fsync;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getMaxWaitTime() {
    return maxWaitTime;
  }

  public boolean isSafe() {
    return safe;
  }

  public boolean isJ() {
    return j;
  }

  public String getUserName() {
    return userName;
  }

  public String getPassWord() {
    return passWord;
  }

  public int getServerSelectionTimeout() {
    return serverSelectionTimeout;
  }

  public boolean isSlaveOk() {
    return slaveOk;
  }

  public void setSlaveOk(boolean slaveOk) {
    this.slaveOk = slaveOk;
  }

}

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
  private String socketKeepAlive = "socketKeepAlive";
  private String socketTimeout = "socketTimeout";
  private String connectionsPerHost = "connectionsPerHost";
  private String threadsAllowedToBlockForConnectionMultiplier = "threadsAllowedToBlockForConnectionMultiplier";
  private String w = "w";
  private String wtimeout = "wtimeout";
  private String fsync = "fsync";
  private String connectTimeout = "connectTimeout";
  private String maxWaitTime = "maxWaitTime";
  private String safe = "safe";
  private String j = "j";
  private String userName = "userName";
  private String passWord = "passWord";
  private String slaveOk = "slaveOk";

  /*
   * mongodb-driver above 3.x , Sets the server selection timeout in
   * milliseconds, which defines how long the driver will wait for server
   * selection to succeed before throwing an exception. default is 30000ms
   */
  private String serverSelectionTimeout = "serverSelectionTimeout";

  public MongoConfig() {

    PropertiesContainer.getInstance().
        loadProperties(InConstants.KURORO_MONGO_PROP, InConstants.KURORO_COMMON_NAMESPACE);

  }

  public boolean isSocketKeepAlive() {
    return Boolean.parseBoolean(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, socketKeepAlive, "true"));
  }

  public int getSocketTimeout() {
    return Integer.parseInt(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, socketTimeout));
  }

  public int getConnectionsPerHost() {
    return Integer.parseInt(PropertiesContainer.getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, connectionsPerHost));
  }

  public int getThreadsAllowedToBlockForConnectionMultiplier() {
    return Integer.parseInt(PropertiesContainer.getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE,
        threadsAllowedToBlockForConnectionMultiplier));
  }

  public int getW() {
    return Integer.parseInt(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, w));
  }

  public int getWtimeout() {
    return Integer.parseInt(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, wtimeout));
  }

  public boolean isFsync() {
    return Boolean.parseBoolean(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, fsync));
  }

  public int getConnectTimeout() {
    return Integer.parseInt(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, connectTimeout));
  }

  public int getMaxWaitTime() {
    return Integer.parseInt(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, maxWaitTime));
  }

  public boolean isSafe() {
    return Boolean.parseBoolean(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, safe));
  }

  public boolean isJ() {
    return Boolean.parseBoolean(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, j));
  }

  public String getUserName() {
    return PropertiesContainer.getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, userName);
  }

  public String getPassWord() {
    return PropertiesContainer.getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, passWord);
  }

  public int getServerSelectionTimeout() {
    return Integer.parseInt(PropertiesContainer.getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE,
        String.valueOf(serverSelectionTimeout)));
  }

  public boolean isSlaveOk() {
    return Boolean.parseBoolean(PropertiesContainer
        .getInstance().getPropertyByNameSpace(InConstants.KURORO_COMMON_NAMESPACE, slaveOk));
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

}

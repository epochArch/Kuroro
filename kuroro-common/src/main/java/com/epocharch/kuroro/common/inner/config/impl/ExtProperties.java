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

package com.epocharch.kuroro.common.inner.config.impl;

import com.epocharch.kuroro.common.inner.config.Operation;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.zkclient.IZkChildListener;
import com.epocharch.zkclient.IZkDataListener;
import com.epocharch.zkclient.ZkClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtProperties {

  private static final Logger LOG = LoggerFactory.getLogger(ExtProperties.class);
  private static final Map<String, ExtProperties> extPropertiesList = new ConcurrentHashMap<String, ExtProperties>();
  private Map<String, Object> cache = new ConcurrentHashMap<String, Object>();
  private List<ExtPropertiesChange> changeList = new ArrayList<ExtPropertiesChange>();
  private ZkClient zkClient = null;

  private ExtProperties(String parentPath) {
    zkClient = KuroroZkUtil.initIDCZk();
    if (!zkClient.exists(parentPath)) {
      zkClient.createPersistent(parentPath, true);
      LOG.warn("Can't find path " + parentPath + " in ZK! Can't find service provider for now");
    }
    observeChild(parentPath);
    List<String> ls = zkClient.getChildren(parentPath);
    observeChildData(parentPath, ls);

  }

  public static ExtProperties getInstance(String parentPath) {

    if (extPropertiesList.containsKey(parentPath)) {
      return extPropertiesList.get(parentPath);
    } else {
      ExtProperties extProperties = new ExtProperties(parentPath);
      extPropertiesList.put(parentPath, extProperties);
      return extProperties;
    }
  }

  private void observeChildData(final String parentPath, List<String> childList) {
    if (childList != null && childList.size() > 0) {
      for (String child : childList) {
        String childPath = KuroroUtil.getChildFullPath(parentPath, child);
        if (zkClient.exists(childPath)) {
          Object result = zkClient.readData(childPath);
          cache.put(child, result);
          observeSpecifyChildData(childPath);
        }
      }
    }
  }

  private void observeChild(String basePath) {
    if (zkClient.exists(basePath)) {
      zkClient.subscribeChildChanges(basePath, new IZkChildListener() {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds)
            throws Exception {
          if (parentPath != null) {
            Object info = null;
            String childPath;
            for (String child : currentChilds) {
              childPath = KuroroUtil.getChildFullPath(parentPath, child);
              if (!cache.containsKey(child)) {
                info = zkClient.readData(childPath, true);
                observeSpecifyChildData(childPath);
                addProperty(child, info, Operation.INSERT);
              }
            }
          }
        }
      });
    }

  }

  private void observeSpecifyChildData(String childPath) {
    zkClient.subscribeDataChanges(childPath, new IZkDataListener() {

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
        if (!KuroroUtil.isBlankString(dataPath)) {
          String child = KuroroUtil.getChildShortPath(dataPath);
          if (cache.containsKey(child)) {
            cache.remove(child);
            addProperty(child, null, Operation.DELETE);
          }
        }
      }

      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
        if (!KuroroUtil.isBlankString(dataPath)) {
          String child = KuroroUtil.getChildShortPath(dataPath);
          Object result = zkClient.readData(dataPath);
          addProperty(child, result, Operation.MODIFY);
        }
      }
    });
  }

  private void addProperty(String key, Object value, Operation oper) {
    if (!oper.equals(Operation.DELETE)) {
      cache.put(key, value);
    }
    addOnChange(key, value, oper);
  }

  private void addOnChange(String key, Object value, Operation oper) {
    if (changeList != null) {
      for (ExtPropertiesChange change : changeList) {
        change.onChange(key, value, oper);
      }
    }

  }

  public void addChange(ExtPropertiesChange change) {
    if (change == null) {
      throw new IllegalArgumentException();
    }
    this.changeList.add(change);
  }

  public Set<String> getKeySet() {
    return cache.keySet();
  }

  public Object getProperty(String key) {
    return cache.get(key);
  }
}

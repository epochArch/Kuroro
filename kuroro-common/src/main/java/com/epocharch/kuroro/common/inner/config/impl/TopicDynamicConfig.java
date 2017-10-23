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

import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.config.ConfigChangeListener;
import com.epocharch.kuroro.common.inner.config.DynamicConfig;
import com.epocharch.kuroro.common.inner.config.Operation;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicDynamicConfig implements DynamicConfig {

  private static final Logger LOG = LoggerFactory.getLogger(TopicDynamicConfig.class);
  private ExtProperties topicConfig;

  public TopicDynamicConfig() {
    String idcTopicPath =
        System.getProperty(InternalPropKey.IDC_KURORO_ZK_ROOT_PATH) + Constants.ZONE_TOPIC;
    try {
      topicConfig = ExtProperties.getInstance(idcTopicPath);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public TopicConfigDataMeta get(String key) {
    return (TopicConfigDataMeta) topicConfig.getProperty(key);
  }

  @Override
  public void setConfigChangeListener(final ConfigChangeListener listener) {
    topicConfig.addChange(new ExtPropertiesChange() {
      @Override
      public void onChange(String key, Object value, Operation oper) {
        listener.onConfigChange(key, value, oper);
      }
    });
  }

  @Override
  public Set<String> getKeySet() {
    return topicConfig.getKeySet();
  }

}

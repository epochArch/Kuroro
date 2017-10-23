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

import com.epocharch.kuroro.monitor.util.MonitorZkUtil;
import com.epocharch.zkclient.ZkClient;
import java.util.List;

/**
 * Created by zhoufeiqiang on 09/05/2017.
 */
public class SyncTopicToIdcZk {

  public SyncTopicToIdcZk() {
  }

  public boolean checkTopicIsExists() {
    boolean topicExists = false;
    String zoneTopicPath = MonitorZkUtil.IDCZkTopicPath;
    ZkClient _zkClient = MonitorZkUtil.getLocalIDCZk();
    if (_zkClient.exists(zoneTopicPath)) {
      List<String> zoneTopicList = _zkClient.getChildren(zoneTopicPath);
      if (zoneTopicList != null && zoneTopicList.size() > 0) {
        topicExists = true;
        return topicExists;
      }
    }
    return topicExists;
  }
}

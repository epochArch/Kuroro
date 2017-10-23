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

package com.epocharch.kuroro.monitor.intelligent.impl;

import com.epocharch.common.util.SystemUtil;
import com.epocharch.kuroro.common.message.MessageLog;
import com.epocharch.kuroro.monitor.plugin.AnalyseUtil;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * @author dongheng
 */
public class AnalystService {

  static final Integer oneHour = 60 * 60 * 1000;
  static final Integer oneDay = 24 * 60 * 60 * 1000;
  static final Integer oneQuarter = 15 * 60 * 1000;
  private static final Logger logger = Logger.getLogger(AnalystService.class);
  static String decollator = "_";
  private static String ip;

  public static int getRandomValue(int max) {
    return (int) (Math.random() * (max + 1) - 1);
  }

  /**
   * 提供给mq发送进出MQ的消息数量 参数格式： [ { topicName:xxx, consumerName:xxxConsumer,
   * in:123, out:234 } ]
   */
  public String sendCounts(List<MessageLog> list) {
    try {
      for (MessageLog mqInfo : list) {
        AnalyseUtil.analyseKuroroInfo(mqInfo);
      }
      return "true";
    } catch (Exception e) {
      e.printStackTrace();
      return "false";
    }
  }

  public String getLocalIP() {
    try {
      if (ip == null) {
        ip = SystemUtil.getLocalhostIp();

      }
    } catch (Exception e) {
      logger.error("AnalystService.getLocalIP() execute error :", e);
    }
    return ip;
  }
}

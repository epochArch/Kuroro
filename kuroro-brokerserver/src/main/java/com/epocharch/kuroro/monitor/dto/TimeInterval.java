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

package com.epocharch.kuroro.monitor.dto;

import com.epocharch.kuroro.monitor.common.DateUtil;
import java.util.Date;

/**
 * 时间间隔枚举
 *
 * @author dongheng
 */
public enum TimeInterval {
  ONE_MINS(1, "1分钟间隔"),

  FIVE_MINS(5, "5分钟间隔"),

  TEN_MINS(10, "5分钟间隔");
  private int code;
  private String desc;

  TimeInterval(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  public static Date getStartTime(Date logTime, TimeInterval timeInterval) {
    if (logTime == null) {
      logTime = new Date();
    }
    int logMins = Integer.valueOf(DateUtil.getMins(logTime));
    int startMins = (logMins / timeInterval.getCode()) * timeInterval.getCode();
    String start = DateUtil.getDateHour(logTime) + ":" + startMins + ":00";
    return DateUtil.getDate(start, DateUtil.DateTimeFormatString);
  }

  public static Date getEndTime(Date logTime, TimeInterval timeInterval) {
    return new Date(
        getStartTime(logTime, timeInterval).getTime() + timeInterval.getCode() * 60 * 1000);
  }

  public int getCode() {
    return code;
  }

  public String getDesc() {
    return desc;
  }

}

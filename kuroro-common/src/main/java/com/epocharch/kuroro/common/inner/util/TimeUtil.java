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

package com.epocharch.kuroro.common.inner.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zhoufeiqiang on 7/5/16.
 */
public class TimeUtil {

  public static String getNowTime() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String date = format.format(new Date());
    return date;
  }

  public static long compare(String nowTime, String lastTime) {
    long value = 0;
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      Date _nowTime = sdf.parse(nowTime);
      Date _lastTime = sdf.parse(lastTime);
      value = _nowTime.getTime() - _lastTime.getTime();
    } catch (ParseException e) {
      value = -1;
      e.printStackTrace();
    } finally {
      return value;
    }
  }

  public static Date timestampToDate(Long currentTime) {
    Date date = null;
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String d = format.format(currentTime);
    try {
      date = format.parse(d);
    } catch (ParseException e) {
      e.printStackTrace();
    } finally {
      return date;
    }
  }

  public static long dateToTimestamp(String dateTime) {
    long timestamp = 0;
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      Date date = format.parse(dateTime);
      timestamp = date.getTime();
    } catch (ParseException e) {
      e.printStackTrace();
    } finally {
      return timestamp;
    }
  }

  /**
   * @return hour unit
   */
  public static int dateTimeCompare(String oldDate, String newDate) {
    int value = 0;
    if (KuroroUtil.isBlankString(newDate)) {
      value = (int) ((System.currentTimeMillis() - TimeUtil.dateToTimestamp(oldDate)) / (60 * 60
          * 1000));
    } else {
      value = (int) ((TimeUtil.dateToTimestamp(newDate) - TimeUtil.dateToTimestamp(oldDate)) / (60
          * 60 * 1000));
    }
    return value;
  }

}

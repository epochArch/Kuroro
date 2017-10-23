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

import org.bson.types.BSONTimestamp;

public class MongoUtil {

  private MongoUtil() {
  }

  public static BSONTimestamp longToBSONTimestamp(Long messageId) {
    int time = (int) (messageId >>> 32);
    int inc = (int) (messageId & 0xFFFFFFFF);
    BSONTimestamp timestamp = new BSONTimestamp(time, inc);
    return timestamp;
  }

  public static Long BSONTimestampToLong(BSONTimestamp timestamp) {
    int time = timestamp.getTime();
    int inc = timestamp.getInc();
    Long messageId = ((long) time << 32) | inc;
    return messageId;
  }

  public static Long getLongByCurTime() {
    int time = (int) (System.currentTimeMillis() / 1000);
    BSONTimestamp bst = new BSONTimestamp(time, 1);
    return BSONTimestampToLong(bst);
  }

  public static long getTimeMillisByLong(Long messageId) {
    BSONTimestamp ts = longToBSONTimestamp(messageId);
    return (long) ts.getTime() * 1000;

  }

  public static BSONTimestamp calMid(Long until, Long newTil) {
    int time1 = (int) (until >>> 32);
    int inc1 = (int) (until & 0xFFFFFFFF);

    int time2 = (int) (newTil >>> 32);
    int inc2 = (int) (newTil & 0xFFFFFFFF);

    return new BSONTimestamp((time1 + time2) >>> 1, (inc1 + inc2) >>> 1);
  }

  public static void main(String[] args) {

    BSONTimestamp timestamp = new BSONTimestamp(1415612177, 243);
    System.out.println(MongoUtil.BSONTimestampToLong(timestamp));
    System.out
        .println("messageId to time...." + MongoUtil.getTimeMillisByLong(6261840461032325378L));
    System.out.println("clientqueue...." + MongoUtil.longToBSONTimestamp(1464060988000L));
    System.out.println(calMid(BSONTimestampToLong(new BSONTimestamp(10, 5)),
        BSONTimestampToLong(new BSONTimestamp(10, 5))).getTime());
  }

}

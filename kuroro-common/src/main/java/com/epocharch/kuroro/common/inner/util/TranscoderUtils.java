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

import java.io.UnsupportedEncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TranscoderUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TranscoderUtils.class);

  public static byte[] encodeLong(long number) {
    long temp = number;
    byte[] b = new byte[8];
    for (int i = 0; i < b.length; i++) {
      b[i] = new Long(temp & 0xff).byteValue();// 将最低位保存在最低位
      temp = temp >> 8;// 向右移8位
    }
    return b;
  }

  public static long decodeLong(byte[] b) {
    long s = 0;
    long s0 = b[0] & 0xff;// 最低位
    long s1 = b[1] & 0xff;
    long s2 = b[2] & 0xff;
    long s3 = b[3] & 0xff;
    long s4 = b[4] & 0xff;// 最低位
    long s5 = b[5] & 0xff;
    long s6 = b[6] & 0xff;
    long s7 = b[7] & 0xff;

    // s0不变
    s1 <<= 8;
    s2 <<= 16;
    s3 <<= 24;
    s4 <<= 8 * 4;
    s5 <<= 8 * 5;
    s6 <<= 8 * 6;
    s7 <<= 8 * 7;
    s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
    return s;

  }

  public static byte[] encodeInt(int num) {
    int temp = num;
    byte[] b = new byte[4];
    for (int i = 0; i < b.length; i++) {
      b[i] = new Integer(temp & 0xff).byteValue();// 将最低位保存在最低位
      temp = temp >> 8;// 向右移8位
    }
    return b;
  }

  public static int decodeInt(byte[] b) {
    int s = 0;
    int s0 = b[0] & 0xff;// 最低位
    int s1 = b[1] & 0xff;
    int s2 = b[2] & 0xff;
    int s3 = b[3] & 0xff;
    s3 <<= 24;
    s2 <<= 16;
    s1 <<= 8;
    s = s0 | s1 | s2 | s3;
    return s;
  }

  public static byte[] encodeString(String str) {

    try {
      byte[] midbytes = str.getBytes("UTF8");
      return midbytes;
    } catch (UnsupportedEncodingException e) {
      LOG.error(e.getMessage(), e.getCause());
    }
    return null;

  }

  public static String decodeString(byte[] b) {
    try {
      return new String(b, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error(e.getMessage(), e.getCause());
    }
    return null;
  }
}

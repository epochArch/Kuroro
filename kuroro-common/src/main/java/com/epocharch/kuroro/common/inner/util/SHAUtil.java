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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHAUtil {

  private SHAUtil() {
  }

  // 根据String生成SHA-1字符串
  public static String generateSHA(String str) {
    try {
      return generateSHA(str.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      return null;
    }
  }

  // 根据bytes生成SHA-1字符串
  public static byte[] generateSHABytes(byte[] bytes) {
    byte[] byteDigest = null;
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byteDigest = md.digest(bytes);
    } catch (NoSuchAlgorithmException nsae) {
      nsae.printStackTrace();
    }
    return byteDigest;
  }

  // 根据bytes生成SHA-1字符串
  private static String generateSHA(byte[] bytes) {
    String ret = null;
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] byteDigest = md.digest(bytes);
      ret = byteToString(byteDigest);
    } catch (NoSuchAlgorithmException nsae) {
      nsae.printStackTrace();
    }
    return ret;
  }

  // 将bytes转化为String
  private static String byteToString(byte[] digest) {
    String tmpStr = "";
    StringBuffer strBuf = new StringBuffer(40);
    for (int i = 0; i < digest.length; i++) {
      tmpStr = (Integer.toHexString(digest[i] & 0xff));
      if (tmpStr.length() == 1) {
        strBuf.append("0" + tmpStr);
      } else {
        strBuf.append(tmpStr);
      }
    }
    return strBuf.toString();
  }
}

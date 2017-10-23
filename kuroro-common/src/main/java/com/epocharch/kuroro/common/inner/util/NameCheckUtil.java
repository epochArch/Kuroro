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

import java.util.regex.Pattern;

public class NameCheckUtil {

  static Pattern topicNamePattern = Pattern.compile("[a-z|A-Z][a-z|A-Z|_|\\-|0-9]{1,29}");

  private NameCheckUtil() {
  }

  /**
   * 判定topicName是否合法
   * <p>
   * <pre>
   * topicName由字母,数字,减号“-”和下划线“_”构成，只能以字母开头，长度为2到30。
   * </pre>
   *
   * @return 合法返回true，非法返回false
   */
  public static boolean isTopicNameValid(String topicName) {
    if (topicName == null || topicName.length() == 0) {
      return false;
    }
    if (topicNamePattern.matcher(topicName).matches()) {
      return true;
    }
    return false;
  }

  /**
   * 判定consumerId是否合法
   * <p>
   * <pre>
   * consumerId由字母,数字,减号“-”和下划线“_”构成，只能以字母开头，长度为2到30。
   * </pre>
   *
   * @return 合法返回true，非法返回false
   */
  public static boolean isConsumerIdValid(String consumerId) {
    if (consumerId == null || consumerId.length() == 0) {
      return false;
    }
    if (consumerId.matches("[a-z|A-Z][a-z|A-Z|_|\\-|0-9]{1,29}")) {
      return true;
    }
    return false;
  }

  public static void main(String[] args) {
    System.out.println(isTopicNameValid("ab"));
    System.out.println(isTopicNameValid("a_"));
    System.out.println(isTopicNameValid("a1"));
    System.out.println(isTopicNameValid("ab_"));
    System.out.println(isTopicNameValid("a1-0"));
    System.out.println(isTopicNameValid("a1234567890123456789"));
    System.out.println(isTopicNameValid("a1."));// false
    System.out.println(isTopicNameValid("a"));// false
    System.out.println(isTopicNameValid("_"));// false
    System.out.println(isTopicNameValid("3"));// false
    System.out.println(isTopicNameValid("a123456789012345678901234567890"));// false
  }
}

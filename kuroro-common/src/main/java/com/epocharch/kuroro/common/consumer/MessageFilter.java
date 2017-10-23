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

package com.epocharch.kuroro.common.consumer;

import java.io.Serializable;
import java.util.Set;

/*
 * 该类用于作消息过滤。<br>
 * 使用<code>createInSetMessageFilter(Set&lt;String&gt; matchTypeSet)</code>
 * 构造实例时，参数matchTypeSet指定了Consumer只消费“Message.type属性包含在matchTypeSet中”的消息
 */
public final class MessageFilter implements Serializable {

  public final static MessageFilter AllMatchFilter = new MessageFilter(FilterType.AllMatch, null);
  /**
   *
   */
  private static final long serialVersionUID = 1984748119414684444L;
  private FilterType type;

  ;
  private Set<String> param;

  private MessageFilter() {
  }

  private MessageFilter(FilterType type, Set<String> param) {
    this.type = type;
    this.param = param;
  }

  public static MessageFilter createInSetMessageFilter(Set<String> matchTypeSet) {
    return new MessageFilter(FilterType.InSet, matchTypeSet);
  }

  public FilterType getType() {
    return type;
  }

  public Set<String> getParam() {
    return param;
  }

  @Override
  public String toString() {
    return "MessageFilter [param=" + param + ", type=" + type + "]";
  }

  public enum FilterType {
    AllMatch, InSet
  }
}

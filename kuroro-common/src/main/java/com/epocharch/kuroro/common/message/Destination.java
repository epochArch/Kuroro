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

package com.epocharch.kuroro.common.message;

import com.epocharch.kuroro.common.inner.util.NameCheckUtil;
import java.io.Serializable;

public class Destination implements Serializable {

  private static final long serialVersionUID = 3833312742447701703L;
  private String name;

  Destination() {
  }

  private Destination(String name) {
    this.name = name.trim();
  }

  public static Destination topic(String name) {
    if (!NameCheckUtil.isTopicNameValid(name)) {
      throw new IllegalArgumentException((new StringBuilder())
          .append(
              "Topic name is illegal, should be [0-9,a-z,A-Z,'_','-'], begin with a letter, and length is 2-30 long\uFF1A")
          .append(name).toString());
    } else {
      return new Destination(name);
    }
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return String.format("Destination [name=%s]", new Object[]{name});
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Destination other = (Destination) obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    return true;
  }
}

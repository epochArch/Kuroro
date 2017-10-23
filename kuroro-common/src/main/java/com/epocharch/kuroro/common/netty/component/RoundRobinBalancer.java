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

package com.epocharch.kuroro.common.netty.component;

import com.epocharch.kuroro.common.constants.Constants;
import java.util.Collection;

public class RoundRobinBalancer extends AbstractBalancer {

  @Override
  public void updateProfiles(Collection<HostInfo> serviceSet) {
    lock.lock();
    try {
      Circle<Integer, HostInfo> circle = new Circle<Integer, HostInfo>();
      int size = 0;
      Collection<HostInfo> realServiceSet = BalancerUtil.filte(serviceSet, whiteList);
      for (HostInfo sp : realServiceSet) {
        circle.put(size++, sp);
      }
      profileCircle = circle;
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected HostInfo doSelect() {
    int key = position.getAndIncrement();
    int totalSize = profileCircle.size();
    int realPos = key % totalSize;
    if (key > Constants.INTEGER_BARRIER) {
      position.set(0);
    }
    return getProfileFromCircle(realPos);
  }

}

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
import com.epocharch.kuroro.common.hash.HashFunction;
import com.epocharch.kuroro.common.hash.HashFunctionFactory;
import java.util.Collection;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConsistentHashBalancer implements ConditionLoadBalancer<HostInfo, String> {

  private Circle<Long, HostInfo> profileCircle = new Circle<Long, HostInfo>();
  private Lock lock = new ReentrantLock();
  private HashFunction hf = HashFunctionFactory.getInstance().getMur2Function();
  private Random random = new Random();
  private Collection<String> whiteList = null;

  @Override
  public HostInfo select() {

    HostInfo sp = null;
    if (profileCircle != null && profileCircle.size() > 0) {
      if (profileCircle.size() == 1) {
        sp = profileCircle.firstVlue();
      } else {
        long code = hf.hash64(System.nanoTime() + "-" + random.nextInt(99));
        sp = getProfileFromCircle(code);
      }
    }
    return sp;
  }

  private HostInfo getProfileFromCircle(Long code) {
    int size = profileCircle.size();
    HostInfo sp = null;
    if (size > 0) {
      Long tmp = code;
      while (size > 0) {
        tmp = profileCircle.lowerKey(tmp);
        sp = profileCircle.get(tmp);
        if (sp != null) {
          break;
        }
        size--;
      }
    }
    return sp;
  }

  @Override
  public void updateProfiles(Collection<HostInfo> serviceSet) {
    lock.lock();
    try {
      Circle<Long, HostInfo> circle = new Circle<Long, HostInfo>();
      Collection<HostInfo> realServiceSet = BalancerUtil.filte(serviceSet, whiteList);
      int totalWeight = getTotalWeight(realServiceSet);
      int size = realServiceSet.size();
      for (HostInfo sp : realServiceSet) {
        int mirror = getMirrorFactor(size, sp.getWeight(), totalWeight, Constants.MIRROR_SEED);
        for (int i = 0; i < mirror; i++) {
          String feed = sp.getConnect() + i;
          long key = hf.hash64(feed);
          put2Circle(key, sp, circle);
        }
      }
      profileCircle = circle;
    } finally {
      lock.unlock();
    }

  }

  private void put2Circle(long key, HostInfo sp, TreeMap<Long, HostInfo> circle) {
    if (circle.containsKey(key)) {
      Long lower = circle.lowerKey(key);
      if (lower == null) {
        key = key / 2;
      } else {
        key = lower + (key - lower) / 2;
      }
      put2Circle(key, sp, circle);
    } else {
      circle.put(key, sp);
    }
  }

  private int getTotalWeight(Collection<HostInfo> serviceSet) {
    int value = 1;
    if (serviceSet != null && serviceSet.size() > 0) {
      for (HostInfo sp : serviceSet) {
        value += sp.getWeight();
      }
    }

    return value;
  }

  private int getMirrorFactor(int size, int weighted, int totalWeight, int seed) {
    int value = totalWeight;
    value = seed * size * weighted / totalWeight;
    return value;
  }

  @Override
  public HostInfo select(String condition) {
    HostInfo sp = null;
    if (profileCircle != null && profileCircle.size() > 0) {
      if (profileCircle.size() == 1) {
        sp = profileCircle.firstVlue();
      } else {
        long code = hf.hash64(condition);
        sp = getProfileFromCircle(code);
      }
    }
    return sp;
  }

  @Override
  public void setWhiteList(Collection<String> serviceSet) {
    this.whiteList = serviceSet;

  }
}

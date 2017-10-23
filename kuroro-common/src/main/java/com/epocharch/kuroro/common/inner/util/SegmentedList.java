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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 分段存放ID <br/>
 * 非线程安全
 *
 * @author bill
 * @date 5/7/14
 */
public class SegmentedList<T> extends AbstractList<List<T>> {

  public static final SegmentedList EMPTY = new SegmentedList();

  private List<List<T>> segments = new ArrayList<List<T>>();
  private int size;

  public SegmentedList() {

  }

  @Override
  public boolean contains(Object o) {
    for (List<T> segment : segments) {
      if (segment.contains(o)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean add(List<T> ts) {
    if (ts == null || ts.isEmpty()) {
      return false;
    }
    boolean is = segments.add(ts);
    if (is) {
      size += ts.size();
    }
    return is;
  }

  public boolean addElement(T t) {
    if (t == null) {
      return false;
    }
    List<T> list = new ArrayList<T>();
    list.add(t);
    return add(list);
  }

  @Override
  public Iterator<List<T>> iterator() {
    return segments.iterator();
  }

  @Override
  public List<T> get(int index) {
    return segments.get(index);
  }

  public int segments() {
    return segments.size();
  }

}

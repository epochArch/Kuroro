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

package com.epocharch.kuroro.monitor.dto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 报表上数据, 注意x和y轴数据是有序存放
 *
 * @author dongheng 下午3:46:29
 */
public class ChartsData implements Serializable {

  private static final long serialVersionUID = 1L;
  private List<Object> xdata = new ArrayList<Object>();
  private List<Object> ydata = new ArrayList<Object>();
  private List<Object> others = new ArrayList<Object>();
  private List<List<Object>> allList = new ArrayList<List<Object>>();
  private List<Object> extInfo = new ArrayList<Object>();

  public List<List<Object>> getAllList() {
    return allList;
  }

  public void setAllList(List<List<Object>> allList) {
    this.allList = allList;
  }

  public List<Object> getXdata() {
    return xdata;
  }

  public void setXdata(List<Object> xdata) {
    this.xdata = xdata;
  }

  public List<Object> getYdata() {
    return ydata;
  }

  public void setYdata(List<Object> ydata) {
    this.ydata = ydata;
  }

  public List<Object> getOthers() {
    return others;
  }

  public void setOthers(List<Object> others) {
    this.others = others;
  }

  /***
   * 组装成下面格式: var line3 = [ [ 'Cup Holder Pinion Bob', 7 ], [ 'Generic Fog
   * Lamp Marketing Gimmick', 9 ], [ 'HDTV Receiver', 15 ], [ '8 Track Control
   * Module', 12 ], [ 'SSPFM (Sealed Sludge Pump Fourier Modulator)', 3 ], [
   * 'Transcender/Spice Rack', 6 ], [ 'Hair Spray Rear View Mirror Danger
   * Indicator', 18 ] ];
   * */
  public List<List<Object>> toList() {
    List<List<Object>> result = new ArrayList<List<Object>>();
    if (xdata == null || ydata == null || xdata.size() != ydata.size()) {
      return result;
    }
    for (int i = 0; i < xdata.size(); i++) {
      List<Object> curt = new ArrayList<Object>();
      curt.add(xdata.get(i));
      curt.add(ydata.get(i));
      result.add(curt);
    }
    this.allList = result;
    return result;
  }

  public List<Object> getExtInfo() {
    return extInfo;
  }

  public void setExtInfo(List<Object> extInfo) {
    this.extInfo = extInfo;
  }
}

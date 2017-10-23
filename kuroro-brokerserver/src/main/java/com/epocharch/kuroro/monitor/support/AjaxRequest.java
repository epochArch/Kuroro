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

package com.epocharch.kuroro.monitor.support;

import org.apache.commons.lang.StringUtils;

public class AjaxRequest {

  static final String DOT = ".";
  private String s;
  private String m;
  private String p;
  private String c;
  private String o;
  private String k;
  private String i;
  private String z;

  public String getS() {
    return s;
  }

  public void setS(String s) {
    this.s = s;
  }

  public String getM() {
    return m;
  }

  public void setM(String m) {
    this.m = m;
  }

  public String getP() {
    return p;
  }

  public void setP(String p) {
    this.p = p;
  }

  public String getServiceId() {
    return s + DOT + m;
  }

  public String getC() {
    return c;
  }

  public void setC(String c) {
    this.c = c;
  }

  public String getO() {
    return o;
  }

  public void setO(String o) {
    this.o = o;
  }

  public String getK() {
    return k;
  }

  public void setK(String k) {
    this.k = k;
  }

  public String getI() {
    return i;
  }

  public void setI(String i) {
    this.i = i;
  }

  public String getZ() {
    return z;
  }

  public void setZ(String z) {
    this.z = z;
  }

  public boolean isValid() {
    if (StringUtils.isEmpty(s) || StringUtils.isEmpty(m) || StringUtils.isEmpty(o) || StringUtils
        .isEmpty(k) || StringUtils
        .isEmpty(c)) {
      return false;
    }

    return true;
  }
}

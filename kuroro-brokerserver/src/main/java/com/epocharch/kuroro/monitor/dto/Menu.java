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

/**
 * 对应数据库的 DO
 *
 * @author hello_yun
 */
public class Menu {

  private long id;
  private long pid;
  private String code;
  private String name;
  private String url;
  private String app;
  private String pname;

  public Menu() {
    super();
  }

  public Menu(long pid, String code, String name, String url) {
    super();
    this.pid = pid;
    this.code = code;
    this.name = name;
    this.url = url;
  }

  public Menu(long pid, String code, String name, String url, String app) {
    super();
    this.pid = pid;
    this.code = code;
    this.name = name;
    this.url = url;
    this.app = app;
  }

  public Menu(String code, String name, String url) {
    super();
    this.code = code;
    this.name = name;
    this.url = url;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getPid() {
    return pid;
  }

  public void setPid(long pid) {
    this.pid = pid;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getApp() {
    return app;
  }

  public void setApp(String app) {
    this.app = app;
  }

  /**
   * @return the pname
   */
  public String getPname() {
    return pname;
  }

  /**
   * @param pname the pname to set
   */
  public void setPname(String pname) {
    this.pname = pname;
  }

}

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

import java.util.ArrayList;
import java.util.List;

/**
 * @author hello_yun
 */
public class MenuManageVO {

  private String id;
  private String pid;
  private String name;
  private String code;
  private String url;
  private String app;
  private boolean checked = false;
  private boolean leaf = false;
  private boolean expanded = false;
  private String iconCls = "task-folder";
  private List<MenuManageVO> children = new ArrayList<MenuManageVO>();

  public MenuManageVO() {
    super();
  }

  public MenuManageVO(String id, String pid, String text) {
    super();
    this.id = id;
    this.pid = pid;
    this.name = text;
  }

  public void addChildMenu(MenuManageVO menu) {
    menu.setLeaf(true);
    this.children.add(menu);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public String getName() {
    return name;
  }

  public void setName(String text) {
    this.name = text;
  }

  public boolean isChecked() {
    return checked;
  }

  public void setChecked(boolean checked) {
    this.checked = checked;
  }

  public boolean isLeaf() {
    return leaf;
  }

  public void setLeaf(boolean leaf) {
    this.leaf = leaf;
  }

  /**
   * @return the code
   */
  public String getCode() {
    return code;
  }

  /**
   * @param code the code to set
   */
  public void setCode(String code) {
    this.code = code;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * @param url the url to set
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * @return the app
   */
  public String getApp() {
    return app;
  }

  /**
   * @param app the app to set
   */
  public void setApp(String app) {
    this.app = app;
  }

  public List<MenuManageVO> getChildren() {
    return children;
  }

  /**
   * @return the iconCls
   */
  public String getIconCls() {
    return iconCls;
  }

  /**
   * @param iconCls the iconCls to set
   */
  public void setIconCls(String iconCls) {
    this.iconCls = iconCls;
  }

  /**
   * @return the expanded
   */
  public boolean isExpanded() {
    return expanded;
  }

  /**
   * @param expanded the expanded to set
   */
  public void setExpanded(boolean expanded) {
    this.expanded = expanded;
  }

}

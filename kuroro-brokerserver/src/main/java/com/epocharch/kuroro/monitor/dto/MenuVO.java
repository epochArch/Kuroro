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
 * 页面左边菜单栏
 *
 * @author hello_yun
 */
public class MenuVO {

  private long id;
  private long pid;
  private boolean leaf = false;
  private String code;
  private String name;
  private String icon;
  private String url;
  private List<MenuVO> menuItems = new ArrayList<MenuVO>();

  public MenuVO() {
  }

  public MenuVO(long id, String menuName, String code, String url) {
    super();
    this.id = id;
    this.name = menuName;
    this.code = code;
    this.url = url;
  }

  public MenuVO(long id, String menuName, String code) {
    super();
    this.id = id;
    this.name = menuName;
    this.code = code;
  }

  public void addChildMenu(MenuVO menu) {
    this.menuItems.add(menu);
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  /**
   * @return the pid
   */
  public long getPid() {
    return pid;
  }

  /**
   * @param pid the pid to set
   */
  public void setPid(long pid) {
    this.pid = pid;
  }

  public boolean isLeaf() {
    return leaf;
  }

  public void setLeaf(boolean leaf) {
    this.leaf = leaf;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getIcon() {
    return icon;
  }

  public void setIcon(String icon) {
    this.icon = icon;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public List<MenuVO> getMenuItems() {
    return menuItems;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (id ^ (id >>> 32));
    return result;
  }

  @Override
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
    MenuVO other = (MenuVO) obj;
    if (id != other.id) {
      return false;
    }
    return true;
  }

}

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
public class MenuTreeVO {

  private long id;
  private long pid;
  private String text;
  private boolean checked = false;
  private boolean leaf = false;
  private List<MenuTreeVO> children = new ArrayList<MenuTreeVO>();

  public MenuTreeVO() {
    super();
  }

  public MenuTreeVO(long id, long pid, String text) {
    super();
    this.id = id;
    this.pid = pid;
    this.text = text;
  }

  public void addChildMenu(MenuTreeVO menu) {
    this.children.add(menu);
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

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
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

  public List<MenuTreeVO> getChildren() {
    return children;
  }

  public void setChildren(List<MenuTreeVO> children) {
    this.children = children;
  }

}

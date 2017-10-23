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

package com.epocharch.kuroro.monitor.author.dao;

import com.epocharch.kuroro.monitor.dto.Menu;
import com.epocharch.kuroro.monitor.dto.MenuManageVO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Repository;

@Repository
public class MenuDao extends BaseDAO<Menu> {

  /**
   * 取得所有1级菜单
   */
  public List<Map> getFirstMenus() {
    return analyseMyIbaitsDAO.queryForList(sql());
  }

  /**
   * 获取父菜单下的所有子菜单
   */
  public List<Map> getSubMenus(int pid) {
    List<Integer> pidList = new ArrayList<Integer>();
    pidList.add(pid);
    return analyseMyIbaitsDAO.queryForList(sql(), pidList);
  }

  public List<Integer> getMenuPids(String menuIds) {
    List<String> menuIdList = new ArrayList<String>();
    menuIdList.add(menuIds);
    return analyseMyIbaitsDAO.queryForList(sql(), menuIdList);
  }

  public List<Menu> getAllMenus() {
    return analyseMyIbaitsDAO.queryForList(sql());
  }

  public Integer addMenu(Menu menu) {
    return (Integer) analyseMyIbaitsDAO.insert(sql(), menu);
  }

  public Integer getMenuNum() {
    return (Integer) analyseMyIbaitsDAO.queryForObject(sql());
  }

  @SuppressWarnings("unchecked")
  public List<MenuManageVO> getParentMenuByPage(int start, int limit) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("offset", start);
    params.put("pagesize", limit);
    return analyseMyIbaitsDAO.queryForList(sql(), params);
  }

  @SuppressWarnings("unchecked")
  public List<MenuManageVO> getParentMenus() {
    return analyseMyIbaitsDAO.queryForList(sql());
  }

  @SuppressWarnings("unchecked")
  public List<MenuManageVO> getSubMenusByPids(String pids) {
    return analyseMyIbaitsDAO.queryForList(sql(), pids);
  }

  public Menu getMenuById(int id) {
    return (Menu) analyseMyIbaitsDAO.queryForObject(sql(), id);
  }

  public int updateMenu(Menu menu) {
    return analyseMyIbaitsDAO.update(sql(), menu);
  }

}

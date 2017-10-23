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
import com.epocharch.kuroro.monitor.dto.Role;
import com.epocharch.kuroro.monitor.dto.RoleAssociation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Repository;

@Repository
public class RoleDao extends BaseDAO<Role> {

  public List<Map> getAllRoles() {
    return analyseMyIbaitsDAO.queryForList(sql());
  }

  public int getRoleNum() {
    return (Integer) analyseMyIbaitsDAO.queryForObject(sql());
  }

  /**
   * 统计指定的role 是否存在
   */
  public int getCountByRoleCode(String roleCode) {
    return (Integer) analyseMyIbaitsDAO.queryForObject(sql(), roleCode);
  }

  @SuppressWarnings("unchecked")
  public List<Role> getRoleList(int start, int limit) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("offset", start);
    params.put("pagesize", limit);
    return analyseMyIbaitsDAO.queryForList(sql(), params);
  }

  public List<RoleAssociation> getRoleAssociationList(long roleId, long pvgType) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("roleId", roleId);
    params.put("pvgType", pvgType);
    return analyseMyIbaitsDAO.queryForList(sql(), params);
  }

  public Role getRoleByCode(String roleCode) {
    return (Role) analyseMyIbaitsDAO.queryForObject(sql(), roleCode);
  }

  public void saveRole(Role role) {
    analyseMyIbaitsDAO.update(sql(), role);
  }

  public void delRole(String roleCodes) {
    analyseMyIbaitsDAO.delete(sql(), roleCodes);
  }

  public void clearPvg(Integer roleId, int pvgType) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("roleId", roleId);
    params.put("pvgType", pvgType);
    analyseMyIbaitsDAO.delete(sql(), params);
  }

  public void delRolePvg(int roleId) {
    analyseMyIbaitsDAO.delete(sql(), roleId);
  }

  public void addRolePvg(Integer roleId, Set<Integer> pvgIds, int pvgType) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("roleId", roleId);
    params.put("pvgType", pvgType);
    for (Integer pvgId : pvgIds) {
      params.put("pvgId", pvgId);
      analyseMyIbaitsDAO.update(sql(), params);
    }
  }

  public String getUserRoleCodesByUid(long uid) {
    return (String) analyseMyIbaitsDAO.queryForObject(sql(), uid);
  }

  public List<Integer> getRoleIdsByRoleCodes(String roleCodes) {
    String[] roleCodeArray = roleCodes.split(",");
    List<String> roleParam = new ArrayList<String>();
    for (int i = 0; i < roleCodeArray.length; i ++) {
      roleParam.add(roleCodeArray[i]);
    }
    return analyseMyIbaitsDAO.queryForList(sql(), roleParam);
  }

  public List<Menu> getPvgByRoleIds(List associationsList) {
    return analyseMyIbaitsDAO.queryForList(sql(), associationsList);
  }

  public List<Integer> getAssociationId(int roleIds) {
    return analyseMyIbaitsDAO.queryForList(sql(), roleIds);
  }
}

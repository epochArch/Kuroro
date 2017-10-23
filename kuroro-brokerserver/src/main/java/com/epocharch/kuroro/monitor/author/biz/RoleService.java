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

package com.epocharch.kuroro.monitor.author.biz;

import com.epocharch.kuroro.monitor.author.dao.RoleDao;
import com.epocharch.kuroro.monitor.common.WebUtils;
import com.epocharch.kuroro.monitor.dto.Menu;
import com.epocharch.kuroro.monitor.dto.MenuVO;
import com.epocharch.kuroro.monitor.dto.Role;
import com.epocharch.kuroro.monitor.log.aop.OperationLogAnnotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Resource;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("prototype")
public class RoleService {

  @Resource
  private RoleDao roleDao;

  public Map<String, Object> getRoles(Map<String, Object> params) {
    if (params == null || params.isEmpty()) {
      return null;
    }
    Integer start = (Integer) params.get("start");
    Integer pageSize = (Integer) params.get("limit");
    start = start == null ? 0 : start;
    pageSize = pageSize == null ? 10 : pageSize;
    int total = this.getRoleNum();
    List<Role> roles = this.getRoleList(start, pageSize);
    Collections.sort(roles, new Comparator<Role>() {
      @Override
      public int compare(Role o1, Role o2) {
        if (o1.getAppName() != null && o2.getAppName() != null) {
          return o2.getAppName().compareTo(o1.getAppName());
        }
        return 0;
      }
    });
    Map<String, Object> resultMap = new HashMap<String, Object>();
    resultMap.put("total", total);
    resultMap.put("role", roles);
    return resultMap;
  }

  @OperationLogAnnotation(moduleName = "角色权限管理", operationName = "新增或修改角色")
  public Map<String, String> updateRole(
      Map<String, Object> params) {
    Map<String, String> result = new HashMap<String, String>();
    if (params == null || params.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    Integer roleId = (Integer) params.get("id");
    String roleCode = (String) params.get("roleCode");
    String roleName = (String) params.get("roleName");
    String appName = (String) params.get("appName");
    if (appName == null) {
      appName = WebUtils.PRODUCTION;
    }
    appName = StringUtils.lowerCase(appName);
    if (StringUtils.trimToNull(roleCode) == null && StringUtils.trimToNull(roleName) == null) {
      result.put("result", "角色名与角色Code不能为空 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    if (roleId != null && roleId > 0) {//修改
      Role role = new Role(roleCode, roleName, appName, roleId);
      result.put("result", "角色已经修改成功 ！");
      roleDao.updateById(role);
      result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    } else {//增加
      Role role = new Role(roleCode, roleName, appName);
      int count = roleDao.getCountByRoleCode(roleCode);
      if (count == 0) {
        roleDao.saveRole(role);
        result.put("result", "角色保存成功");
        result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
      } else {
        result.put("result", "您要添加的角色已经存在，请添加系统中不存在的角色");
        result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      }
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  @OperationLogAnnotation(moduleName = "角色权限管理", operationName = "删除角色")
  public Map<String, String> delRole(
      Map<String, Object> params) {
    Map<String, String> result = new HashMap<String, String>();
    if (params == null || params.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    List<Integer> ids = (List<Integer>) params.get("roleIds");
    if (ids == null || ids.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    for (Integer roleId : ids) {
      roleDao.deleteById(roleId);
      roleDao.delRolePvg(roleId);//同时删除对应的权限列表
    }
    result.put("result", "删除角色成功 ！");
    result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    return result;
    //roleDao.delRole(roleCodes);

  }

  public List<Map> getAllRoles() {
    return roleDao.getAllRoles();
  }

  public List<Role> getRoleList(int start, int limit) {
    return roleDao.getRoleList(start, limit);
  }

  public int getRoleNum() {
    return roleDao.getRoleNum();
  }

  /**
   * 给角色分配权限,权限有2个维度-菜单,hdb
   *
   * @param
   */
  @SuppressWarnings("unchecked")
  @OperationLogAnnotation(moduleName = "角色权限管理", operationName = "分配权限")
  public Map<String, String> assignPvg(
      Map<String, Object> params) {
    Map<String, String> result = new HashMap<String, String>();
    Integer roleId = (Integer) params.get("roleId");
    List<Integer> idList = (List<Integer>) params.get("menuIds");
    Integer pvgType = (Integer) params.get("pvgType");
    if (roleId == null || idList == null || idList.isEmpty() || pvgType == null) {
      result.put("result", "请传入要操作的参数 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    roleDao.clearPvg(roleId, pvgType);
    if (pvgType == 1) {//说明是菜单权限
      Set<Integer> menuIdSet = new HashSet<Integer>();
      for (Integer id : idList) {
        menuIdSet.add(id);
      }
      roleDao.addRolePvg(roleId, menuIdSet, pvgType);
    }
    result.put("result", "分配权限成功 ！");
    result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    return result;
  }

  /**
   * 根据用户id获得用户角色code信息
   */

  public String getUserRoleCodesByUid(long uid) {
    return roleDao.getUserRoleCodesByUid(uid);
  }

  public String getRoleIdsByRoleCodes(String roleCodes) {
    List<Integer> roleIds = roleDao.getRoleIdsByRoleCodes(roleCodes);
    String res = "";
    for (int i = 0; i < roleIds.size(); i++) {
      if (i != roleIds.size() - 1) {
        res += roleIds.get(i) + ",";
      } else {
        res += roleIds.get(i);
      }

    }
    return res;
  }

  /**
   * 获得该角色id的权限url列表
   */
  public List<MenuVO> getMenuByRoleIds(String roleIds) {
    List<Integer> assocationIdList = roleDao.getAssociationId(Integer.parseInt(roleIds));
    List<Menu> menuDao = roleDao.getPvgByRoleIds(assocationIdList);
    Map<Long, MenuVO> menuMap = new HashMap<Long, MenuVO>();
    for (Menu menu : menuDao) {
      long pid = menu.getPid();
      String code = menu.getCode();
      String urlMapping = WebUtils.getUrlByCode(code);
      String url = (urlMapping != null ? urlMapping : menu.getUrl());
      MenuVO vo = new MenuVO(menu.getId(), menu.getName(), code, url);

      if (menuMap.get(pid) == null) {
        menuMap.put(menu.getId(), vo);
      } else {
        menuMap.get(pid).addChildMenu(vo);
      }
    }

    List<MenuVO> resultList = new ArrayList<MenuVO>();
    for (Entry<Long, MenuVO> vo : menuMap.entrySet()) {
      resultList.add(vo.getValue());
    }
    return resultList;
  }

}

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

import com.epocharch.kuroro.monitor.author.dao.MenuDao;
import com.epocharch.kuroro.monitor.author.dao.RoleDao;
import com.epocharch.kuroro.monitor.common.WebUtils;
import com.epocharch.kuroro.monitor.dto.Menu;
import com.epocharch.kuroro.monitor.dto.MenuManageVO;
import com.epocharch.kuroro.monitor.dto.MenuTreeVO;
import com.epocharch.kuroro.monitor.log.aop.OperationLogAnnotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Resource;
import javax.swing.tree.DefaultMutableTreeNode;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("prototype")
public class MenuService {

  @Resource
  private MenuDao menuDao;
  @Resource
  private RoleDao roleDao;

  public List<Menu> getAllMenus() {
    return menuDao.getAllMenus();
  }

  public Map<String, Object> getMenuNum() {
    Map<String, Object> map = new HashMap<String, Object>();
    int total = menuDao.getMenuNum();
    map.put("total", total);
    return map;
  }

  public Map<String, Object> getPmenus() {
    List<MenuManageVO> pmenus = menuDao.getParentMenus();
    Map<String, Object> menuMap = new HashMap<String, Object>();
    menuMap.put("rows", pmenus);
    return menuMap;
  }

  public List<MenuManageVO> getMenus() {
    List<MenuManageVO> pmenus = menuDao.getParentMenus();
    List<MenuManageVO> resultList = new ArrayList<MenuManageVO>();
    if (pmenus == null) {
      return resultList;
    }
    StringBuilder sb = new StringBuilder();
    Map<String, MenuManageVO> menuMap = new HashMap<String, MenuManageVO>();
    for (MenuManageVO m : pmenus) {
      String id = m.getId();
      sb.append(id).append(",");
      menuMap.put(id, m);
    }
    String pids = sb.toString();
    pids = pids.substring(0, pids.lastIndexOf(","));
    List<MenuManageVO> subMenus = menuDao.getSubMenusByPids(pids);
    if (subMenus != null) {
      for (MenuManageVO menu : subMenus) {
        if (menuMap.get(menu.getPid()) != null) {
          menuMap.get(menu.getPid()).addChildMenu(menu);
        }
      }
    }
    Collections.sort(pmenus, new Comparator<MenuManageVO>() {
      @Override
      public int compare(MenuManageVO o1, MenuManageVO o2) {
        if (o1.getId() != null && o2.getId() != null) {
          return Integer.valueOf(o1.getId()).compareTo(Integer.valueOf(o2.getId()));
        }
        return 0;
      }
    });
    return pmenus;
  }

  @OperationLogAnnotation(moduleName = "菜单管理", operationName = "新增菜单")
  public Map<String, String> addMenu(Map<String, Object> params) {
    Map<String, String> result = new HashMap<String, String>();
    if (params == null || params.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    String name = (String) params.get("menu_name");
    String code = (String) params.get("code");
    String url = (String) params.get("url");
    Object obj = params.get("pid");
    Long pid = 0L;
    if (obj != null) {
      pid = Long.valueOf(obj.toString());
    }
    String app = WebUtils.PRODUCTION;
    Menu menu = new Menu(pid, code, name, url, app);
    menuDao.addMenu(menu);
    result.put("result", "操作已完成！");
    result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    return result;
  }

  public Menu getMenuById(int id) {
    Menu menu = menuDao.getMenuById(id);
    if (menu.getPid() != 0) {
      Long pid = menu.getPid();
      Menu pmenu = menuDao.getMenuById(pid.intValue());
      menu.setPname(pmenu.getName());
    }
    return menu;
  }

  @OperationLogAnnotation(moduleName = "菜单管理", operationName = "修改菜单")
  public Map<String, String> updateMenu(Map<String, Object> params) {
    Map<String, String> result = new HashMap<String, String>();
    if (params == null || params.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    String name = (String) params.get("name");
    String code = (String) params.get("code");
    String url = (String) params.get("url");
    Integer id = (Integer) params.get("id");
    Integer pd = (Integer) params.get("pid");
    long pid = 0;
    if (pd != null) {
      pid = Long.valueOf(pd);
    }
    Menu menu = new Menu(pid, code, name, url);
    menu.setId(id);
    menuDao.updateMenu(menu);
    result.put("result", "操作已完成！");
    result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    return result;
  }

  @OperationLogAnnotation(moduleName = "菜单管理", operationName = "删除菜单")
  public Map<String, String> deleteMenu(Map<String, Object> params) {
    Map<String, String> result = new HashMap<String, String>();
    if (params == null || params.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    Integer id = (Integer) params.get("id");
    int menuId = id.intValue();
    menuDao.deleteById(menuId);
    result.put("result", "操作已完成！");
    result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    return result;
  }

  public List<MenuTreeVO> getMenuTree(String roleId) {
    //该权限拥有的菜单
//    List<Menu> existMenu = roleDao.getPvgByRoleIds(roleId);
    List<Integer> assocationIdList = roleDao.getAssociationId(Integer.parseInt(roleId));
    List<Menu> existMenu = roleDao.getPvgByRoleIds(assocationIdList);
    List<Long> exitIds = new ArrayList<Long>();
    if (existMenu != null && !existMenu.isEmpty()) {
      for (Menu m : existMenu) {
        exitIds.add(m.getId());
      }
    }
    //查出所有的菜单
    List<Menu> allMenus = menuDao.getAllMenus();

    Map<Long, MenuTreeVO> menuMap = new HashMap<Long, MenuTreeVO>();
    for (Menu menu : allMenus) {
      long pid = menu.getPid();
      long id = menu.getId();
      MenuTreeVO vo = new MenuTreeVO(id, pid, menu.getName());
      if (exitIds.contains(id)) {
        vo.setChecked(true);
      }
      if (menuMap.get(pid) == null) {
        menuMap.put(menu.getId(), vo);
      } else {
        vo.setLeaf(true);
        menuMap.get(pid).addChildMenu(vo);
      }
    }

    List<MenuTreeVO> resultList = new ArrayList<MenuTreeVO>();
    for (Entry<Long, MenuTreeVO> vo : menuMap.entrySet()) {
      resultList.add(vo.getValue());
    }

    return resultList;
  }

  public List<Map> getFirstMenus() {
    return menuDao.getFirstMenus();
  }

  /**
   * 获得用户指定菜单下的所有子菜单
   */
  public List<Map> getSubMenus(int pid, Map menuPvg) {
    List<Map> menuList = menuDao.getSubMenus(pid);
    return filterMenus(menuList, menuPvg);
  }

  public List<Map> filterMenus(List<Map> menuList, Map menuPvg) {
    List<Map> res = new ArrayList();
    List<Integer> menuPvgList = (List<Integer>) menuPvg.get("pvgMenuIds");// 获得该角色菜单id权限
    for (Map menu : menuList) {
      int menuId = (Integer) menu.get("id");
      if (menuPvgList.contains(menuId)) {
        res.add(menu);
      }
    }
    return res;
  }

  /**
   * 构建完整的菜单树
   */
  public DefaultMutableTreeNode buildCompleteTree() {
    List<Menu> list = getAllMenus();

    DefaultMutableTreeNode rootNode = new DefaultMutableTreeNode(getRootItem());

    Map<Long, DefaultMutableTreeNode> firstLevel = new HashMap<Long, DefaultMutableTreeNode>();

    for (Menu item : list) {
      String code = item.getCode();
      long mid = item.getId();
      long pid = item.getPid();
      if (code == null || code.trim().equals("")) {
        continue;
      }

      boolean firstLevelFlag = true;// 默认一级菜单
      if (pid > 0) {// 非一级菜单,>=2级,有父菜单说明不是一级菜单
        firstLevelFlag = false;
      }
      DefaultMutableTreeNode lookForNode = rootNode;
      if (firstLevelFlag) {// 1级菜单
        DefaultMutableTreeNode firstLevelNode = new DefaultMutableTreeNode(item);
        rootNode.add(firstLevelNode);
        firstLevel.put(mid, firstLevelNode);
        continue;
      } else {// 2级及2级以上菜单
        if (firstLevel.get(pid) != null) {// 2级菜单
          lookForNode = firstLevel.get(pid);
          lookForNode.add(new DefaultMutableTreeNode(item));
          continue;
        } else {// 2级以上菜单

          Enumeration enu = lookForNode.depthFirstEnumeration();
          while (enu.hasMoreElements()) {// 找出该菜单节点的父菜单
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) enu.nextElement();
            Object ob = node.getUserObject();
            if (!(ob instanceof Menu)) {
              continue;
            }
            Menu tmpItem = (Menu) ob;
            if (tmpItem.getId() == pid) {// 得到该菜单节点的父节点;
              node.add(new DefaultMutableTreeNode(item));
              break;
            }
          }
        }

      }
    }
    return rootNode;
  }

  public void insertMenu(Menu menu) {
    menuDao.addMenu(menu);
  }

  private Menu getRootItem() {
    Menu root = new Menu();
    root.setCode("");
    root.setName("系统菜单");
    root.setUrl("#");

    return root;
  }

}

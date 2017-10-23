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

import com.epocharch.kuroro.monitor.author.dao.UserDao;
import com.epocharch.kuroro.monitor.common.WebUtils;
import com.epocharch.kuroro.monitor.dto.User;
import com.epocharch.kuroro.monitor.log.aop.OperationLogAnnotation;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Scope("prototype")
@Service
public class UserService {

  @Resource
  private UserDao userDao;

  public List<Map> getAllUsers() {
    return userDao.getAllUsers();
  }

  public Map<String, Object> getUserByPage(Map<String, Integer> params) {
    Integer start = params.get("start");
    start = start == null ? 0 : start;
    Integer pageSize = params.get("limit");
    int total = getUserNum();
    List<User> userList = getUserList(start, pageSize);
    Collections.sort(userList, new Comparator<User>() {
      @Override
      public int compare(User o1, User o2) {
        if (o1.getRoleCodes() != null && o2.getRoleCodes() != null) {
          return o2.getRoleCodes().compareTo(o1.getRoleCodes());
        }
        return 0;
      }
    });

    Map<String, Object> resultMap = new HashMap<String, Object>();
    resultMap.put("total", total);
    resultMap.put("result", userList);
    return resultMap;
  }

  @OperationLogAnnotation(moduleName = "用户角色管理", operationName = "新增用户")
  public Map<String, String> userUpdate(Map<String, Object> map) {
    Map<String, String> result = new HashMap<String, String>();
    if (map == null || map.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    Integer userId = (Integer) map.get("userId");
    String username = (String) map.get("name");
    User user = new User();
    user.setName(username);
    if (userId != null && userId > 0) {
      user.setId(userId);
      userDao.updateById(user);
      result.put("result", "已经完成用户修改 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    } else {
      int count = userDao.getCountByUserName(username);
      if (count == 0) {
        userDao.addUser(user);
        result.put("result", "已经完成添加新用户 ！");
        result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
      } else {
        result.put("result", "您要添加的新用户已经存在 ！");
        result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      }
    }
    return result;
  }

  public void delUser(int uid) {
    userDao.deleteById(uid);
    userDao.delUserRole(uid);//同时删除对应的角色列表

  }

  @OperationLogAnnotation(moduleName = "用户角色管理", operationName = "删除用户")
  public Map<String, String> delUsers(
      Map<String, Object> uidMaps) {
    Map<String, String> result = new HashMap<String, String>();
    if (uidMaps == null || uidMaps.isEmpty()) {
      result.put("result", "请传入要操作的参数 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    Object userIds = uidMaps.get("uid");
    if (userIds == null) {
      return result;
    }
    List<Integer> ids = (List) userIds;
    for (Integer id : ids) {
      userDao.deleteById(id);
      userDao.delUserRole(id);//同时删除对应的角色列表
    }
    result.put("result", "已经完成用户删除 ！");
    result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    return result;
  }

  public User getUserByName(String uname) {
    return userDao.getUserByName(uname);
  }

  public List<User> getUserList(int start, int limit) {
    return userDao.getUserList(start, limit);
  }

  public int getUserNum() {
    return userDao.getUserNum();
  }

  @OperationLogAnnotation(moduleName = "用户角色管理", operationName = "分配角色")
  public Map<String, String> roleUpdate(
      Map<String, Object> params) {
    Map<String, String> result = new HashMap<String, String>();
    Integer uid = (Integer) params.get("userId");
    List<String> roleCodes = (List<String>) params.get("roleCodes");
    if (uid == null || roleCodes == null) {
      result.put("result", "请传入要操作的参数 ！");
      result.put("resultCode", WebUtils.RESULT_CODE_FAILURE_VALUE);
      return result;
    }
    StringBuilder sb = new StringBuilder();
    for (String code : roleCodes) {
      sb.append(code).append(",");
    }
    String str = sb.toString();
    str = str.substring(0, sb.lastIndexOf(","));
    List userRoleList = userDao.getUserRole(uid);
    if (userRoleList != null && userRoleList.size() > 0) {
      userDao.roleUpdate(uid, str);
    } else {
      userDao.addUserRole(uid, str);
    }
    result.put("result", "已经完成角色分配 ！");
    result.put("resultCode", WebUtils.RESULT_CODE_SUCCESS_VALUE);
    return result;
  }

}

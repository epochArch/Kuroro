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

import com.epocharch.kuroro.monitor.dto.User;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Repository;

@Repository
public class UserDao extends BaseDAO<User> {

  public List<Map> getAllUsers() {
    return analyseMyIbaitsDAO.queryForList(sql());
  }

  public void addUser(User user) {
    if (user == null) {
      throw new IllegalArgumentException("无效的用户数据信息");
    }
    User tmpUser = getUserByName(user.getName());
    if (tmpUser != null) {
      throw new RuntimeException("用户已经存在");
    }
    saveUser(user);
  }

  public User getUserByName(String userName) {
    return (User) analyseMyIbaitsDAO.queryForObject(sql(), userName);
  }

  public void saveUser(User user) {
    analyseMyIbaitsDAO.update(sql(), user);
  }

  public void delUser(String uids) {
    List<String> uidsList = new ArrayList<String>();
    uidsList.add(uids);
    analyseMyIbaitsDAO.delete(sql(), uidsList);
  }

  public void delUserRole(int uid) {
    //		throw new RuntimeException("delete user role failure ... ");
    analyseMyIbaitsDAO.delete(sql(), uid);
  }

  public List<User> getUserList(int start, int limit) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("offset", start);
    params.put("pagesize", limit);
    return analyseMyIbaitsDAO.queryForList(sql(), params);
  }

  public int getUserNum() {
    return (Integer) analyseMyIbaitsDAO.queryForObject(sql());
  }

  public int getCountByUserName(String name) {
    return (Integer) analyseMyIbaitsDAO.queryForObject(sql(), name);
  }

  public int roleUpdate(int uid, String roleCodes) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("uid", uid);
    params.put("roleCodes", roleCodes);
    return analyseMyIbaitsDAO.update(sql(), params);

  }

  public List getUserRole(int uid) {
    return analyseMyIbaitsDAO.queryForList(sql(), uid);
  }

  public int addUserRole(int uid, String roleCodes) {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("uid", uid);
    params.put("roleCodes", roleCodes);
    return analyseMyIbaitsDAO.update(sql(), params);
  }

}

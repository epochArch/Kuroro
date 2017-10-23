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

package com.epocharch.kuroro.monitor.author.login;

import com.alibaba.fastjson.JSON;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.monitor.author.biz.RoleService;
import com.epocharch.kuroro.monitor.author.biz.UserService;
import com.epocharch.kuroro.monitor.common.Env;
import com.epocharch.kuroro.monitor.common.LDAP;
import com.epocharch.kuroro.monitor.common.WebUtils;
import com.epocharch.kuroro.monitor.dto.MenuVO;
import com.epocharch.kuroro.monitor.dto.User;
import com.epocharch.kuroro.monitor.intelligent.impl.AnalystJobService;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.apache.commons.lang.StringUtils;

/**
 * @author hello_yun
 */
public class LoginServlet extends HttpServlet {

  private static final long serialVersionUID = -1021638606906395376L;
  private static final String productionURI = "ldapidc.yihaodian.com:389";
  private static final String othersURI = "192.168.100.16:389";
  private static final String dns = "yihaodian.com";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String method = req.getParameter("method");
    if ("login".equals(method)) {
      this.login(req, resp);
      return;
    } else if ("menus".equals(method)) {
      this.getMenus(req, resp);
      return;
    } else if ("loginOut".equals(method)) {
      this.loginOut(req, resp);
    }
  }

  private boolean checkUser(HttpServletRequest req, HttpServletResponse resp) {
    String username = req.getParameter("username");
    String password = req.getParameter("password");
    try {
      if (StringUtils.trimToNull(username) == null || StringUtils.trimToNull(password) == null) {
        return false;
      } else if (username.equals(InternalPropKey.ADMIN_NAME) && password.equals(InternalPropKey.ADMIN_PASS)) {
        return true;
      } else {
        LDAP ldap = new LDAP();
        String ldapConnUrl = othersURI;
        if (AnalystJobService.isProduction()) {
          ldapConnUrl = productionURI;
        }
        int isPassed = ldap.connect(username, password, ldapConnUrl, dns);
        if (isPassed != 0) {
          return false;
        } else {
          return true;
        }
      }
    } catch (Exception e) {
    }
    return false;
  }

  private void login(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String path = req.getContextPath();
    if (!checkUser(req, resp)) {
      resp.setContentType("application/json;charset=UTF-8");
      req.setAttribute("msg", "用户名密码不正确，请检查! ");
      try {
        req.getRequestDispatcher("/login.jsp").forward(req, resp);
      } catch (ServletException e) {
      }
      return;
    }
    String username = req.getParameter("username");
    UserService userService = (UserService) Env.getBean("userService");
    User user = userService.getUserByName(username);
    String userRoleCodes = WebUtils.DEFAULT_ROLE;
    RoleService roleService = (RoleService) Env.getBean("roleService");
    if (user == null) {
      user = new User(username);
    } else {
      userRoleCodes = roleService.getUserRoleCodesByUid(user.getId());// 用户角色
    }

    userRoleCodes = userRoleCodes == null ? WebUtils.DEFAULT_ROLE : userRoleCodes;
    if (!userRoleCodes.startsWith(WebUtils.PRODUCTION)) {
      userRoleCodes = WebUtils.DEFAULT_ROLE;
    }
    String roleIds = roleService.getRoleIdsByRoleCodes(userRoleCodes);

    HttpSession session = req.getSession();
    user.setRoleCodes(userRoleCodes);
    user.setRoleIds(roleIds);
    session.setAttribute("user", user);
    resp.sendRedirect(path + "/index.html");
  }

  private void getMenus(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    User user = (User) WebUtils.getLoginUser(req);
    resp.setContentType("application/json;charset=UTF-8");
    PrintWriter out = resp.getWriter();
    if (user == null) {
      resp.sendError(999);
      return;
    }
    String roleIds = user.getRoleIds();
    RoleService roleService = (RoleService) Env.getBean("roleService");
    if (roleIds == null || "".equals(roleIds)) {
      roleIds = roleService.getRoleIdsByRoleCodes(WebUtils.DEFAULT_ROLE);
    }
    List<MenuVO> menuList = roleService.getMenuByRoleIds(roleIds);// 包含菜单id权限和菜单url权限;
    String menuStr = JSON.toJSONString(menuList);
    out.write(menuStr);
    out.flush();
    if (out != null) {
      out.close();
    }
  }

  private void loginOut(HttpServletRequest req, HttpServletResponse resp) {
    req.getSession().invalidate();
    try {
      resp.sendError(999);
    } catch (IOException e) {
    }
  }

}

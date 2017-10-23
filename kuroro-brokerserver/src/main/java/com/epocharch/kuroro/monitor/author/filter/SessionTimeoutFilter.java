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

package com.epocharch.kuroro.monitor.author.filter;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class SessionTimeoutFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    HttpSession session = httpRequest.getSession();
    session.setMaxInactiveInterval(24 * 60 * 60);
    StringBuffer requestURL = httpRequest.getRequestURL();
    String loginUrl = requestURL.substring(0, requestURL.lastIndexOf("/") + 1);

    if (httpRequest.getServletPath().startsWith("/page") || httpRequest.getServletPath()
        .startsWith("/ajax.do")) {//kuroro-monitor页面功能的url
      if (session.getAttribute("user") == null) {
        //请求为Ajax请求
        if (httpRequest.getHeader("x-requested-with") != null && httpRequest
            .getHeader("x-requested-with")
            .equalsIgnoreCase("XMLHttpRequest")) {
          httpResponse.addHeader("sessionStatus", "ajaxTimeout");
          httpResponse.addHeader("loginUrl", loginUrl);
          chain.doFilter(request, response);
        } else {
          //请求为http请求
          httpResponse.addHeader("sessionStatus", "httpTimeout");
          httpResponse.addHeader("loginUrl", loginUrl);
          chain.doFilter(request, response);
        }
      } else {
        chain.doFilter(request, response);
      }
    } else {
      chain.doFilter(request, response);
    }

  }

  @Override
  public void destroy() {

  }

}

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

package com.epocharch.kuroro.common.jmx.support;

import com.epocharch.common.util.SystemUtil;
import java.net.MalformedURLException;
import javax.management.remote.JMXServiceURL;

public class JMXServiceURLBuilder {

  private static final String DEFAULT_URL_PATH_NAME = "/jmxrmi";

  private String host;

  private int port;

  private String protocol = "rmi";

  private String urlPathProtocol = "/jndi/rmi://";

  private String urlPathName = DEFAULT_URL_PATH_NAME;

  public JMXServiceURLBuilder(int port) {
    this(port, DEFAULT_URL_PATH_NAME);
  }

  public JMXServiceURLBuilder(int port, String pathName) {
    this.port = port;
    this.urlPathName = pathName.startsWith("/") ? pathName : "/" + pathName;
  }

  public JMXServiceURLBuilder(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getJMXServiceURLToString() {
    return this.getJMXServiceURL().toString();
  }

  public JMXServiceURL getJMXServiceURL() {
    try {
      checkHost();
      return new JMXServiceURL(protocol, host, port, getUrlPath());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Build JMXServiceURL faild, cause: ", e);
    }
  }

  private void checkHost() {
    if (this.host == null) {
      String firstNoLoopbackIP = SystemUtil.getLocalhostIp();

      this.host = firstNoLoopbackIP != null ? firstNoLoopbackIP : "0.0.0.0";
    }
  }

  public String getUrlPath() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(this.urlPathProtocol);
    stringBuilder.append(this.host);
    stringBuilder.append(":");
    stringBuilder.append(this.port);
    stringBuilder.append(this.urlPathName);
    return stringBuilder.toString();
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getUrlPathProtocol() {
    return urlPathProtocol;
  }

  public void setUrlPathProtocol(String urlPathProtocol) {
    this.urlPathProtocol = urlPathProtocol;
  }

  public String getUrlPathName() {
    return urlPathName;
  }

  public void setUrlPathName(String urlPathName) {
    this.urlPathName = urlPathName;
  }
}

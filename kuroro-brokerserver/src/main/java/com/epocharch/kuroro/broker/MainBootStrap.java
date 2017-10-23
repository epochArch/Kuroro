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

package com.epocharch.kuroro.broker;

import com.epocharch.kuroro.broker.constants.BrokerInConstants;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.inner.util.KuroroZkUtil;
import com.epocharch.kuroro.monitor.common.Env;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.PropertyConfigurator;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MainBootStrap {

  public static final int monitor_port = 8080;

  private MainBootStrap() {

  }

  public static void main(String[] args) {
    System.out.println("production====" + Constants.PRODUCTION.toLowerCase());
    String log4j = MainBootStrap.class.getClassLoader().getResource("log4j-pro.properties").getFile();
    System.out.println("Loading log4j config files : " + log4j);
    PropertyConfigurator.configureAndWatch(log4j, 20 * 1000);

    KuroroZkUtil.initBrokerIDCMetas();
    KuroroZkUtil.initClientIDCMetas();
    SyncTopicToIdcZk syncTopicToIdcZk = new SyncTopicToIdcZk();
    boolean topicsExists = syncTopicToIdcZk.checkTopicIsExists();

    if (topicsExists && (args == null || args.length <= 0)) {
      ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
          new String[]{"applicationContext.xml", "applicationContext-leader.xml",
              "applicationContext-producer.xml",
              "applicationContext-consumer.xml"});
      Env.setApplicationContext(context);
    } else if (topicsExists && (args.length > 0)) {
      List<String> commands = new ArrayList<String>();
      commands.add("applicationContext.xml");
      for (String command : args) {
        if (command.equals("-l")) {
          commands.add("applicationContext-leader.xml");
        }
        if (command.equals("-p")) {
          commands.add("applicationContext-producer.xml");
        }
        if (command.equals("-c")) {
          commands.add("applicationContext-consumer.xml");
        }
      }
      String[] strs = new String[commands.size()];
      new ClassPathXmlApplicationContext(commands.toArray(strs));
    } else if (!topicsExists) {
      new ClassPathXmlApplicationContext(new String[]{"applicationContext-monitor.xml"});
      System.out.println("=====current only start kuroro console , not start broker server !=====");
    }

    System.out.println("*******************Spring init finished********************");
    //启动http server
    try {
      Server server = new Server(monitor_port);

      WebAppContext wac = new WebAppContext();
      String webappDir = System.getProperty("webapp.dir", BrokerInConstants.KURORO_WEB_DIR);
      wac.setResourceBase(webappDir);
      wac.setDescriptor(wac.getResourceBase() + "WEB-INF/web.xml");
      wac.setContextPath(Constants.PRODUCTION.toLowerCase());
      wac.setParentLoaderPriority(true);
      server.setHandler(wac);
      server.start();
      System.out.println("################Jetty Server init finished#################");
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

}

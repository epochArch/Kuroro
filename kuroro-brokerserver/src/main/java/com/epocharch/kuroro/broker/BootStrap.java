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

import com.epocharch.kuroro.common.inner.util.LoadKuroroPropertes;
import com.epocharch.kuroro.monitor.dao.BaseMybatisDAO;
import com.epocharch.kuroro.monitor.dto.KuroroConsumerAnalyse;
import com.epocharch.kuroro.monitor.dto.Menu;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by zhoufeiqiang on 16/10/2017.
 */
public class BootStrap {

  @Autowired
  BaseMybatisDAO analyseMyIbaitsDAO;


  public void testDao() {
    ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
        new String[]{"applicationContext-monitor-test.xml"});

    analyseMyIbaitsDAO = (BaseMybatisDAO) context.getBean("analyseMyIbaitsDAO");

    int a =100;
    List<Integer> ids = analyseMyIbaitsDAO.queryForList("RoleDao.getAssociationId", a);

    for (Integer id : ids) {
      System.out.println("a===" + id);
    }

    List<Menu> value = analyseMyIbaitsDAO.queryForList("RoleDao.getPvgByRoleIds", ids);

    for (Menu menu : value) {
      System.out.println("value===" + value);
    }
  }

  public void test(){
    ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
        new String[]{"applicationContext-monitor.xml"});

    String resource = "mybatis-anlaysis-config.xml";
    InputStream inputStream = null;
    try {
      inputStream = LoadKuroroPropertes.provider().loadFileFromClasspath(resource);
    } catch (IOException e) {
      e.printStackTrace();
    }
    SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);

    SqlSession session = sqlSessionFactory.openSession();
    try {
      KuroroConsumerAnalyse kuroroConsumerAnalyse = session.selectOne("monitor_kuroro_consumer_analyse_day.getById");
      System.out.println("juroro===" + kuroroConsumerAnalyse);
    } finally {
      session.close();
    }


  }

  public static void main(String [] args) {
    new BootStrap().testDao();
  }

}

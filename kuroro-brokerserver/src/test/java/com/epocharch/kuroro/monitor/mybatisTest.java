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

package com.epocharch.kuroro.monitor;

import com.epocharch.kuroro.common.inner.util.LoadKuroroPropertes;
import com.epocharch.kuroro.monitor.dao.BaseMybatisDAO;
import com.epocharch.kuroro.monitor.dto.KuroroConsumerAnalyse;
import java.io.IOException;
import java.io.InputStream;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by zhoufeiqiang on 16/10/2017.
 */
public class mybatisTest {

  @Autowired
  BaseMybatisDAO analyseMyIbaitsDAO;


  @Test
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

}

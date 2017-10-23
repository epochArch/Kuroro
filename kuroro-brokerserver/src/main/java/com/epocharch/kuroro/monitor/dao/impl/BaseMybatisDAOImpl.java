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

package com.epocharch.kuroro.monitor.dao.impl;

import com.epocharch.kuroro.monitor.dao.BaseMybatisDAO;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

/**
 * Created by zhoufeiqiang on 17/10/2017.
 */
public class BaseMybatisDAOImpl implements BaseMybatisDAO {

  private SqlSession sqlSession;

  public void setSqlSession(SqlSession sqlSession) {
    this.sqlSession = sqlSession;
  }

  @Override
  public void batchInsert(String sqlAlias, List list) {
    for (int i = 0; i < list.size(); i++) {
      Object content = list.get(i);
      sqlSession.insert(sqlAlias, content);
    }
  }

  @Override
  public Object insert(String statementName, Object parameterObject) {
    Integer count = sqlSession.insert(statementName, parameterObject);
    return count;
  }

  @Override
  public <T> List<T> queryForList(String statementName, Object param, Class<T> clazz) {
    return sqlSession.selectList(statementName, param);
  }

  @Override
  public <T> List<T> queryForList(String statementName, Class<T> clazz) {
    return sqlSession.selectList(statementName);
  }

  @Override
  public int delete(String statementName) {
    return sqlSession.delete(statementName);
  }

  @Override
  public int delete(String statementName, Object parameterObject) {
    return sqlSession.delete(statementName, parameterObject);
  }

  @Override
  public int update(String statementName, Object parameterObject) {
    return sqlSession.update(statementName, parameterObject);
  }

  @Override
  public Object queryForObject(String statementName) {
    return sqlSession.selectOne(statementName);
  }

  @Override
  public Object queryForObject(String statementName, Object parameterObject) {
    return sqlSession.selectOne(statementName, parameterObject);
  }

  @Override
  public List queryForList(String statementName, Object parameterObject) {
    return sqlSession.selectList(statementName, parameterObject);
  }

  @Override
  public List queryForList(String statementName) {
    return sqlSession.selectList(statementName);
  }

}

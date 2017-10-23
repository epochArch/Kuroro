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

package com.epocharch.kuroro.monitor.dao;

import java.util.List;

/**
 * Created by zhoufeiqiang on 17/10/2017.
 */
public interface BaseMybatisDAO {

  public void batchInsert(final String sqlAlias, final List list);

  public Object insert(final String statementName, final Object parameterObject);

  public <T> List<T> queryForList(String statementName, Object param, Class<T> clazz);

  public <T> List<T> queryForList(String statementName, Class<T> clazz);

  public int delete(String statementName);

  public int delete(final String statementName, final Object parameterObject);

  public int update(final String statementName, final Object parameterObject);

  public Object queryForObject(final String statementName);

  public Object queryForObject(final String statementName, final Object parameterObject);

  public List queryForList(final String statementName, final Object parameterObject);

  public List queryForList(final String statementName);
  }

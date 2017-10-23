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

import com.epocharch.kuroro.monitor.dao.impl.BaseMybatisDAOImpl;
import java.util.HashMap;
import javax.annotation.Resource;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

public abstract class BaseDAO<E> {

  protected final Logger log = Logger.getLogger(getClass());

  @Resource(name = "analyseMyIbaitsDAO")
  protected BaseMybatisDAOImpl analyseMyIbaitsDAO;

  public Integer insertReturnId(E entity) {
    Assert.notNull(entity);
    Integer id = (Integer) analyseMyIbaitsDAO.insert(sql(), entity);
    return id;
  }

  public Integer deleteById(Integer id) {
    Assert.notNull(id);
    return (Integer) analyseMyIbaitsDAO.delete(sql(), id);
  }

  public Integer updateById(E param) {
    Assert.notNull(param);
    return (Integer) analyseMyIbaitsDAO.update(sql(), param);
  }

  public E queryById(Integer id) {
    Assert.notNull(id);
    return (E) analyseMyIbaitsDAO.queryForObject(sql(), id);
  }

  protected String sql() {
    String sqlId = new Throwable().getStackTrace()[1].getMethodName();
    return getClass().getSimpleName() + '.' + sqlId;
  }

  public String getEntityClassName() {
    return this.getClass().getSimpleName();
  }

  public Integer insert(E entity) {
    String className1 = this.getEntityClassName();
    String className2 = entity.getClass().getSimpleName();
    String sm = className1 + ".insert" + className2;
    return (Integer) analyseMyIbaitsDAO.insert(sm, entity);
  }

  public void update(E entity) {
    String className1 = this.getEntityClassName();
    String className2 = entity.getClass().getSimpleName();
    String sm = className1 + ".update" + className2;
    analyseMyIbaitsDAO.update(sm, entity);
  }

  public E get(HashMap param) {
    String className = this.getEntityClassName();
    String query = className + ".get" + this.getEntityClass().getSimpleName();
    E l = (E) analyseMyIbaitsDAO.queryForObject(query, param);
    return l;
  }

  public Class<E> getEntityClass() {
    return null;
  }
}

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

package com.epocharch.kuroro.monitor.plugin;

import com.epocharch.kuroro.monitor.dto.TimeInterval;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * 流计算对象插件接口
 *
 * @author dongheng
 */

/**
 * 收集客户端Plugin集合
 **/
public interface AnalysePlugin {

  /**
   * MQ Plugin集合
   **/
  Set<AnalysePlugin> messagePlugin = new HashSet<AnalysePlugin>();
  Set<Class> pluginSet = new HashSet<Class>();

  /**
   * 明确当前数据是否需要进行分析
   */
  public boolean accept(Object object);

  /***
   * 分析逻辑实现
   *
   * @param object
   * @return
   */
  public AnalysePlugin analyse(Object object);

  /**
   * 注册当前插件
   */
  public void register();

  /**
   * 将分析结果变成数据持久化对象
   */
  public Object toEntity();

  /**
   * 获取时间间隔
   */
  public TimeInterval getTimeInterval();

  /**
   * 创建 一个实例方法
   */
  public AnalysePlugin createInstance();

  /**
   * 获取统计开始时间
   */
  public Date getStartTime();

  /**
   * 获取统计结束时间
   */
  public Date getEndTime();
}

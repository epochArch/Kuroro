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

package com.epocharch.kuroro.broker.leaderserver;

import com.epocharch.kuroro.common.consumer.MessageFilter;
import com.epocharch.kuroro.common.inner.dao.AckDAO;
import com.epocharch.kuroro.common.inner.dao.CompensationDao;
import com.epocharch.kuroro.common.inner.dao.MessageDAO;
import com.epocharch.kuroro.common.inner.message.KuroroMessage;
import com.epocharch.kuroro.common.inner.strategy.KuroroThreadFactory;
import com.epocharch.kuroro.common.inner.util.MongoUtil;
import com.epocharch.kuroro.common.jmx.support.JmxSpringUtil;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang.math.RandomUtils;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * 补偿者 逻辑：<br/>
 * count x 分钟内的msg和ack，数量相同则认为未丢，否则二分法处理
 *
 * @author bill
 * @date 5/4/14
 */
public class Compensator {

  private static final Logger logger = LoggerFactory.getLogger(Compensator.class);
  private AckDAO ackDAO;
  private CompensationDao compensationDao;
  private MessageDAO messageDAO;
  private ScheduledExecutorService executorService;
  private ReentrantLock taskLock = new ReentrantLock();

  /**
   *
   */
  private Map<String, Semaphore> semaphoreMap = new ConcurrentHashMap<String, Semaphore>();

  /**
   * TODO:在补偿时考虑消息过滤器
   */
  private Map<String, MessageFilter> filterMap = new ConcurrentHashMap<String, MessageFilter>();

  private Map<String, AtomicBoolean> compensatingMap = new ConcurrentHashMap<String, AtomicBoolean>();
  /**
   * 补偿周期，秒
   */
  private int period;

  /**
   * delay时间内的补偿不予执行，秒
   */
  private int delay;

  /**
   * 一段消息数量少于此值时，不再细分，直接按条比对
   */
  private int compareThreshold;

  /**
   * 二分取中值时，多余此值不使用skip，因为skip大量记录效率低
   */
  private int skipThreshold;

  /**
   * 每个topic同时执行的补偿数
   */
  private int permits;

  /**
   * 每次补偿permit的获取超时时间
   */
  private int permitTimeout;

  /**
   * 为了控制补偿开销，ack丢失条数过多，超过阀值，忽略此次补偿
   */
  private int ignoreThreshold;

  /**
   * 一轮补偿的最大时间段,分钟
   */
  private int maxInterval;

  /**
   * 补偿推迟开始，防止补偿堆积的未消费消息
   */
  private int postpone;

  private ConcurrentHashMap<String, ScheduledFuture> scheduledTasks = new ConcurrentHashMap<String, ScheduledFuture>();

  /**
   * 已补偿的但未确认补偿成功的
   * <p>
   * 先判断这个list是否为空再持久化进度，否则补偿进度停留在当前时间，直到无未确认补偿
   * </p>
   */
  private ConcurrentHashMap<String, ConcurrentLinkedQueue<List<Long>>> compensatedIdLists = new ConcurrentHashMap<String, ConcurrentLinkedQueue<List<Long>>>();

  /**
   * 被消费的id list，用以第二轮检查是否全部正常消费
   */
  private ConcurrentHashMap<String, List<List<Long>>> consumedIdLists = new ConcurrentHashMap<String, List<List<Long>>>();

  private Map<String, Long> untilMap = new HashMap<String, Long>();

  private AtomicBoolean started = new AtomicBoolean(false);
  private JmxSpringUtil jmxSpringUtil;

  public Compensator() {
    period = Integer.parseInt(System.getProperty("kuroro.compensation.period", "60"));// sec
    postpone = Integer
        .parseInt(System.getProperty("kuroro.compensation.postpone", "300"));// sec TODO:再延长点
    delay = Integer.parseInt(System.getProperty("kuroro.compensation.delay", "300"));// sec
    compareThreshold = Integer
        .parseInt(System.getProperty("kuroro.compensation.compare.threshold", "50"));// messages
    skipThreshold = Integer
        .parseInt(System.getProperty("kuroro.compensation.skip.threshold", "1000"));
    permits = Integer.parseInt(System.getProperty("kuroro.compensation.topic.permits", "5"));
    permitTimeout = Integer
        .parseInt(System.getProperty("kuroro.compensation.topic.permit.timeout", "5"));// sec
    ignoreThreshold = Integer
        .parseInt(System.getProperty("kuroro.compensation.ignore.threshold", "50000"));// messgaes
    maxInterval = Integer
        .parseInt(System.getProperty("kuroro.compensation.max.interval", "5"));// min
  }

  public void start() {
    if (this.started.compareAndSet(false, true)) {
      logger.info("Starting compensation...");
      executorService = Executors
          .newScheduledThreadPool(10, new KuroroThreadFactory("compensator"));
    }
  }

  public List<Long> getCompensated(String topic, String consumer, String zone) {
    String key = keyFrom(topic, consumer, zone);
    ConcurrentLinkedQueue<List<Long>> queue = compensatedIdLists.get(key);
    if (queue != null) {
      return queue.peek();
    }
    return null;
  }

  public void compensated(String topic, String consumer, Object ref, String zone) {
    String key = keyFrom(topic, consumer, zone);
    if (compensatedIdLists.containsKey(key)) {
      ConcurrentLinkedQueue<List<Long>> tmp = compensatedIdLists.get(key);
      List<List<Long>> consumed = consumedIdLists.get(key);
      List<Long> get = tmp.peek();
      if (get == ref) {
        consumed.add(tmp.poll());
      }

    }
  }

  /**
   * 提交补偿作业
   */
  public void compensate(String topic, String consumer, String zone) {
    taskLock.lock();
    try {
      String key = keyFrom(topic, consumer, zone);
      if (scheduledTasks.contains(key)) {
        boolean isCancelled = scheduledTasks.get(key).cancel(true);
        if (!isCancelled) {
          logger.error("Last task cancel failed[topic={},consumer={},zone=" + zone + "]", topic,
              consumer);
        }
      }
      if (!this.compensatedIdLists.containsKey(key)) {
        ConcurrentLinkedQueue<List<Long>> queue = new ConcurrentLinkedQueue<List<Long>>();
        this.compensatedIdLists.put(key, queue);
      }
      ConcurrentLinkedQueue<List<Long>> idList = compensatedIdLists.get(key);

      if (!this.consumedIdLists.containsKey(key)) {
        List<List<Long>> list = new ArrayList<List<Long>>();
        this.consumedIdLists.put(key, list);
      }
      List<List<Long>> consumedIdList = consumedIdLists.get(key);

      // 创建semaphore
      if (!semaphoreMap.containsKey(topic)) {
        semaphoreMap.put(topic, new Semaphore(this.permits));
      }
      Semaphore semaphore = semaphoreMap.get(topic);

      if (!compensatingMap.containsKey(key)) {
        compensatingMap.put(key, new AtomicBoolean());
      }

      long initialDelay = postpone + RandomUtils.nextInt(120);
      CompensationTask task = new CompensationTask(topic, consumer, idList, consumedIdList,
          semaphore, zone);
      logger.debug("Task created[topic={},consumer={},zone=" + zone + "]", topic, consumer);
      ScheduledFuture future = this.executorService
          .scheduleWithFixedDelay(task, initialDelay, period, TimeUnit.SECONDS);

      logger.info("Start to compensate[topic={},consumer={},zone=" + zone + "]", topic, consumer);

      scheduledTasks.put(key, future);
    } finally {
      taskLock.unlock();
    }
  }

  private String keyFrom(String topic, String consumer, String zone) {
    return zone == null ? consumer + "@" + topic : consumer + "@" + topic + "@" + zone;
  }

  public void stopCompensation(String topic, String consumer, String zone) {
    String key = keyFrom(topic, consumer, zone);
    if (!scheduledTasks.containsKey(key)) {
      return;
    }
    ScheduledFuture future = scheduledTasks.get(key);
    if (future != null && !future.isDone() && !future.isCancelled()) {
      boolean b = future.cancel(true);
      if (b) {
        logger.info("Compensation cancelled[topic={},consumer={},zone=" + zone + "] ", topic,
            consumer);
      } else {
        logger.info("Failed to cancel compensation[topic={},consumer={},zone=" + zone + "] ", topic,
            consumer);
      }
    }
    clean(key);

  }

  public void resetCompensation(String topic, String consumer, String zone) {
    stopCompensation(topic, consumer, zone);
    this.compensationDao.deleteCompensation(topic, consumer, zone);
  }

  private void clean(String key) {
    this.scheduledTasks.remove(key);
    this.compensatingMap.remove(key);
    this.semaphoreMap.remove(key);
    this.consumedIdLists.remove(key);
    this.compensatedIdLists.remove(key);
    this.untilMap.remove(key);
  }

  public void close() {
    logger.info("Close compensation...");

    this.started.compareAndSet(true, false);
    executorService.shutdownNow();
    this.compensatedIdLists.clear();
    this.semaphoreMap.clear();
    this.compensatingMap.clear();
    this.consumedIdLists.clear();
    this.scheduledTasks.clear();
    this.untilMap.clear();
  }

  public boolean isInRound(String topic, String consumer, String zone) {
    String key = keyFrom(topic, consumer, zone);
    return isCompensating(topic, consumer, zone) && compensatingMap.containsKey(key)
        && compensatingMap.get(key).get();
  }

  public boolean isCompensating(String topic, String consumer, String zone) {
    return this.scheduledTasks.containsKey(keyFrom(topic, consumer, zone));
  }

  public AckDAO getAckDAO() {
    return ackDAO;
  }

  public void setAckDAO(AckDAO ackDAO) {
    this.ackDAO = ackDAO;
  }

  public CompensationDao getCompensationDao() {
    return compensationDao;
  }

  public void setCompensationDao(CompensationDao compensationDao) {
    this.compensationDao = compensationDao;
  }

  public MessageDAO getMessageDAO() {
    return messageDAO;
  }

  public void setMessageDAO(MessageDAO messageDAO) {
    this.messageDAO = messageDAO;
  }

  // ///////////JMX部分//////////////
  public void setJmxSpringUtil(JmxSpringUtil jmxSpringUtil) {
    this.jmxSpringUtil = jmxSpringUtil;
    String mBeanName = "Compensator";
    jmxSpringUtil.registerMBean(mBeanName, new MonitorBean(this));
  }

  @Deprecated
  public enum UntilFinderType {
    BY_LENGTH, BY_TIME
  }

  @ManagedResource(description = "compensation info")
  public static class MonitorBean {

    private final WeakReference<Compensator> compensator;

    private MonitorBean(Compensator compensator) {
      this.compensator = new WeakReference<Compensator>(compensator);
    }

    @ManagedAttribute
    public String getCompensating() {
      if (this.compensator.get() != null) {
        StringBuilder builder = new StringBuilder();
        for (String key : compensator.get().scheduledTasks.keySet()) {
          builder.append(key).append(",");
        }
        return builder.toString();
      }
      return null;
    }

    @ManagedAttribute
    public Boolean getStatus() {
      if (this.compensator.get() != null) {
        return compensator.get().started.get();
      }
      return null;
    }

    @ManagedOperation
    @ManagedOperationParameters({
        @ManagedOperationParameter(name = "topic", description = "topic name"),
        @ManagedOperationParameter(name = "consumer", description = "consumer id")})
    @ManagedOperationParameter(name = "zone", description = "zone")
    public String getCompensatingList(
        String topic, String consumer, String zone) {
      if (this.compensator.get() != null) {
        String key = (zone == null ? consumer + "@" + topic : consumer + "@" + topic + "@" + zone);
        return compensator.get().compensatedIdLists.get(key).toString();
      }
      return null;
    }

    @ManagedOperation
    @ManagedOperationParameters({
        @ManagedOperationParameter(name = "topic", description = "topic name"),
        @ManagedOperationParameter(name = "consumer", description = "consumer id")})
    @ManagedOperationParameter(name = "zone", description = "zone")
    public String getUntilOf(
        String topic, String consumer, String zone) {
      if (this.compensator.get() != null) {
        String key = (zone == null ? consumer + "@" + topic : consumer + "@" + topic + "@" + zone);
        Long l = compensator.get().untilMap.get(key);
        BSONTimestamp bs = MongoUtil.longToBSONTimestamp(l);
        return bs.getTime() + ":" + bs.getInc();
      }
      return null;
    }

    @ManagedOperation
    @ManagedOperationParameters({
        @ManagedOperationParameter(name = "topic", description = "topic name"),
        @ManagedOperationParameter(name = "consumer", description = "consumer id")})
    @ManagedOperationParameter(name = "zone", description = "zone")
    public Boolean stopCompensation(
        String topic, String consumer, String zone) {
      if (this.compensator.get() != null) {
        compensator.get().stopCompensation(topic, consumer, zone);
        return compensator.get().isCompensating(topic, consumer, zone);
      }
      return null;
    }

    @ManagedOperation
    @ManagedOperationParameters({
        @ManagedOperationParameter(name = "topic", description = "topic name"),
        @ManagedOperationParameter(name = "consumer", description = "consumer id")})
    @ManagedOperationParameter(name = "zone", description = "zone")
    public Boolean resetCompensation(
        String topic, String consumer, String zone) {
      if (this.compensator.get() != null) {
        compensator.get().resetCompensation(topic, consumer, zone);
        return compensator.get().isCompensating(topic, consumer, zone);
      }
      return null;
    }

    @ManagedOperation
    @ManagedOperationParameters({
        @ManagedOperationParameter(name = "topic", description = "topic name"),
        @ManagedOperationParameter(name = "consumer", description = "consumer id")})
    @ManagedOperationParameter(name = "zone", description = "zone")
    public Boolean restartCompensation(
        String topic, String consumer, String zone) {
      if (this.compensator.get() != null) {
        if (compensator.get().isCompensating(topic, consumer, zone)) {
          compensator.get().stopCompensation(topic, consumer, zone);
        }
        compensator.get().compensate(topic, consumer, zone);
        return compensator.get().isCompensating(topic, consumer, zone);
      }
      return null;
    }

  }

  public class CompensationTask implements Runnable {

    private final List<List<Long>> consumedIdList;
    final private ConcurrentLinkedQueue<List<Long>> compensatedIdList;
    private final Semaphore semaphore;
    private String topic;
    private String consumer;
    private Long until = 0L;
    private UntilFinderType finderType = UntilFinderType.BY_TIME;
    private AtomicBoolean flag = new AtomicBoolean(false);
    private String zone;

    public CompensationTask(String topic, String consumer,
        ConcurrentLinkedQueue<List<Long>> compensatedIdList,
        List<List<Long>> consumedIdList, Semaphore semaphore, String zone) {
      this.compensatedIdList = compensatedIdList;
      this.consumedIdList = consumedIdList;
      this.topic = topic;
      this.consumer = consumer;
      this.semaphore = semaphore;
      this.zone = zone;
      compensatingMap.put(keyFrom(topic, consumer, zone), flag);

      BSONTimestamp ts = compensationDao.getCompensationId(topic, consumer, zone);
      if (ts == null) {// 尚未开始补偿
        logger.info("No compensation history[topic={},consumer={},zone=" + zone + "]", topic,
            consumer);
        Long maxAckId = ackDAO.getMaxMessageID(topic, consumer, 0, zone);
        if (maxAckId == null) {
          // 尚未开始ack，此时跳过此次调度
          // 逻辑里先判断until是否为0，为0直接跳过
        } else {
          // 已有ack，但尚未开始补偿，不考虑id从0开始，因为需要补偿的量可能很大，或者压根不需要补偿历史消息
          // 对于之前没有补偿功能的topic，在某一时刻开启时，为了防止从头开始计算补偿，可以考虑将补偿区间限制在一天内
          // 但考虑到开启补偿后，更多的是针对后续的消息进行补偿，且为方便起见，暂时实现为从最大的ackId开始补偿
          until = maxAckId;
        }
      } else {
        until = MongoUtil.BSONTimestampToLong(ts);
        // 如果补偿点后的消息的ack已被覆盖，不回溯，直接从最小的ack开始
        Long min = ackDAO.getMinMessageID(topic, consumer, 0, zone);

        if (min != null) {
          until = adjustUntil(until, min);
        }// else不考虑这种诡异情况
      }
    }

    //各种tricky，就别看了
    private long adjustUntil(long until, long min) {
      if (min > until) {//ack都被覆盖了，为了能活下去，从1小时之前开始
        logger.info("min > until[topic={},consumer={},zone=" + zone + "]", topic, consumer);
        long anHourAgo = MongoUtil.BSONTimestampToLong(
            new BSONTimestamp((int) (System.currentTimeMillis() / 1000 - 60 * 60), 0));
        if (anHourAgo < min) {
          logger.info("anHourAgo < min[topic={},consumer={},zone=" + zone + "]", topic, consumer);
          return MongoUtil.getLongByCurTime();//这个太极限了
        } else {//an hour ago > min
          return anHourAgo;
        }
      } else {//min < until，如果相差不足1小时，也认为太久了，直接从当前时间开始吧
        if (MongoUtil.longToBSONTimestamp(until).getTime() - MongoUtil.longToBSONTimestamp(min)
            .getTime() < 60 * 60) {
          logger.info("until - min < 1 hour[topic={},consumer={},zone=" + zone + "]", topic,
              consumer);
          return MongoUtil.getLongByCurTime();//这个也太极限了
        } else {
          return until;
        }
      }
    }

    @Override
    public void run() {
      try {
        boolean willStart = semaphore.tryAcquire(permitTimeout, TimeUnit.SECONDS);
        if (!willStart) {
          logger.info("Compensation not permitted[topic={},consumer={},zone=" + zone + "]", topic,
              consumer);
          return;
        }
        boolean begin = flag.compareAndSet(false, true);
        if (!begin) {
          return;
        }
        if (until.longValue() == 0L) {// 尚未开始ack，直接跳过本轮补偿
          logger
              .warn("No ack ,skip the compensation[topic={},consumer={},zone=" + zone + "]", topic,
                  consumer);
          // query new until
          Long tmp = ackDAO.getMinMessageID(topic, consumer, 0, zone);
          if (tmp != null) {
            until = tmp;
          }
          return;
        } else {
          Long min = 0L;
          Object obj = ackDAO.getMinMessageID(topic, consumer, 0, zone);
          if (obj != null) {
            min = (Long) obj;
          }
          long newUntil = adjustUntil(until, min);
          if (until != newUntil) {
            logger.warn(
                "Adjust until from " + until + " to " + newUntil + "[topic={},consumer={},zone="
                    + zone + "] ", topic,
                consumer);
            compensatedIdList.clear();
            consumedIdList.clear();
          }
          until = newUntil;
        }

        // 更新jmx查询标记
        untilMap.put(keyFrom(topic, consumer, zone), until);
        // 开始本轮补偿逻辑
        // 上次补偿如果是delay min之内，跳过一轮
        int nowSec = (int) (System.currentTimeMillis() / 1000);
        int untilSec = MongoUtil.longToBSONTimestamp(until).getTime();
        int pe = nowSec - untilSec;
        logger.debug("Now {}, until {}", nowSec, untilSec);
        if (pe > 0 && pe < delay) {// 离上次完成补偿不太久，不足delay时间，跳过本轮
          logger.info("Not enough delay time[topic={},consumer={},zone=" + zone + "] ", topic,
              consumer);
          return;
        }

        if (!this.compensatedIdList.isEmpty()) {
          logger.info("Not yet resend all messages[topic={},consumer={},zone=" + zone + "] ", topic,
              consumer);
          return;
        }
        if (this.consumedIdList.isEmpty()) {
          logger.debug("Empty compensation list[topic={},consumer={},zone=" + zone + "] ", topic,
              consumer);
        } else {
          // 挨个检查是否已补偿成功
          Iterator<List<Long>> it = consumedIdList.iterator();
          while (it.hasNext()) {
            List<Long> ids = it.next();
            Iterator<Long> iit = ids.iterator();
            while (iit.hasNext()) {
              Long id = iit.next();
              if (ackDAO.isAcked(topic, consumer, id, 0, zone)) {
                ;
              } else {
                KuroroMessage msg = messageDAO.getMessage(topic, id);
                if (msg
                    != null) {// 如果当前msg还存在，仍然未有ack信息，才进行再次补偿（场景：消息堆积很久MSG已清除，导致永远没有ACK信息，补偿进程卡住在某个时间点场景）
                  List<Long> tmp = new ArrayList<Long>();// 小概率事件，干脆单条发了
                  tmp.add(id);
                  compensatedIdList.add(tmp);
                  logger.debug(
                      "A msg not compensated successfully[topic={},consumer={},id=" + id + ",zone="
                          + zone + "]",
                      topic, consumer);
                } else {
                  logger.debug(
                      "A msg not find ack and msg !![topic={},consumer={},id=" + id + ",zone="
                          + zone + "]",
                      topic, consumer);
                }
              }
            }
          }
          consumedIdList.clear();
        }

        if (!this.compensatedIdList.isEmpty()) {
          // 补偿完还有未ack，则认为补偿失败，此轮补偿提前结束，等待再次补偿结果
          // TODO:重试次数限制是否要加
          logger.warn(
              sizeOf(compensatedIdList)
                  + " messages was NOT compensate successfully[topic={},consumer={},zone=" + zone
                  + "]",
              topic, consumer);
          return;
        } else {
          // 标记进度
          logger.debug(
              sizeOf(compensatedIdList) + " Compensation id updated to " + until
                  + "[topic={},consumer={},zone=" + zone + "]",
              topic, consumer);
          compensationDao
              .updateCompensationId(topic, consumer, MongoUtil.longToBSONTimestamp(until), zone);
        }

        // 首先根据until找出下界
        Long newTil = findUntil(until);
        if (newTil.longValue() == 0L) {
          logger.debug("No newTil found[topic={},consumer={},zone=" + zone + "]", topic, consumer);
          return;
        }

        // 补偿until->newTil之间的消息
        doCompensation(until, newTil, compensatedIdList);
        int size = sizeOf(compensatedIdList);
        logger.info(
            "One round done,{} lost messages in {} segments found[" + topic + "," + consumer + "]",
            size,
            compensatedIdList.size());

        until = newTil;// 将newTil置为until，下一轮择机持久化补偿进度
      } catch (InterruptedException e) {
        logger.error("Compensation interrupted[topic={},consumer={},zone=" + zone + "]", topic,
            consumer);
        e.printStackTrace();
      } catch (Throwable t) {
        logger.error(
            "Compensation task error[topic=" + topic + ",consumer=" + consumer + ",zone=" + zone
                + "]", t);
        t.printStackTrace();
      } finally {
        semaphore.release();
        boolean b = flag.compareAndSet(true, false);
        if (!b) {
          flag.set(false);
        }
      }
    }

    /**
     * 补偿until到newTil之间的消息，左闭区间，右开区间
     *
     * @param from 下限，包含
     * @param to 上限，不包含
     */
    private void doCompensation(Long from, Long to,
        ConcurrentLinkedQueue<List<Long>> toBeCompensated) {
      BSONTimestamp fromTs = MongoUtil.longToBSONTimestamp(from);
      BSONTimestamp toTs = MongoUtil.longToBSONTimestamp(to);
      String fromStr = fromTs.getTime() + ":" + fromTs.getInc();
      String toStr = toTs.getTime() + ":" + toTs.getInc();

      logger.debug("Begin to compensate messages between " + fromStr + " and " + toStr
              + " [topic={},consumer={},zone=" + zone + "]",
          topic, consumer);
      if (from.longValue() == to.longValue()) {
        logger.warn("Same from and to {}", MongoUtil.longToBSONTimestamp(from));
        return;
      }
      // 获取区间内的数量
      int msgCount = messageDAO.countBetween(topic, from, to);
      if (msgCount == 0) {
        logger.debug("No message between " + from + " and " + to + "[topic={},consumer={}]", topic,
            consumer);
        return;// 无消息
      }
      int ackCount = ackDAO.countBetween(topic, consumer, from, to, 0, zone);
      if (ackCount == 0) {// 毫无确认，全部加入到待补偿
        List<Long> ids = messageDAO.getMessageIdBetween(topic, from, to);
        logger.debug(
            "No ack between " + from + " and " + to + "[topic={},consumer={},zone=" + zone + "]",
            topic, consumer);

        toBeCompensated.add(ids);
        logger.debug("Compensation list increases to " + sizeOf(toBeCompensated)
                + "[topic={},consumer={},zone=" + zone + "]",
            topic, consumer);
        return;
      }
      logger.debug("{} msgs and {} acks", msgCount, ackCount);

      int lost = msgCount - ackCount;// 丢失数量
      if (lost > ignoreThreshold) {// 丢失超过阀值，放弃补偿，需要注意将补偿延迟较大时间开始调度，以便让消费先进行一会，否则长久不消费后会直接跳过丢失条目
        // TODO:是否有更好的保护机制
        logger.info("Too much msg lost,ignore this round[topic={},consumer={},zone=" + zone + "]",
            topic, consumer);
        return;
      }
      if (lost == 0) {
        // 这一段没丢消息，大多数情况下
        logger.debug("No ack lost[topic={},consumer={},zone=" + zone + "]", topic, consumer);
        return;
      } else if (msgCount < ackCount) {
        // 理论上不可能
        logger.error("More ack than msg[topic={},consumer={},zone=" + zone + "]", topic, consumer);
      } else {
        int compensatedCount = sizeOf(toBeCompensated);
        // msgCount > ackCount
        // 存在未ack的msg
        // 二分定位之
        if (msgCount <= compareThreshold) {
          logger.debug(
              "Too few msg,check one by one[count=" + msgCount + "topic={},consumer={},zone=" + zone
                  + "]", topic,
              consumer);
          // 超阀值，挨个判断是否ack
          List<Long> ids = messageDAO.getMessageIdBetween(topic, from, to);
          // 保持表中的顺序分配
          boolean isContinue = false;
          List<Long> segment = null;
          for (Long id : ids) {
            if (!ackDAO.isAcked(topic, consumer, id, zone)) {// 无ack
              isContinue = true;
              if (segment == null) {
                segment = new ArrayList<Long>();
              }
              // 不检查id是否已存在
              segment.add(id);
            } else {// 有ack
              if (isContinue) {
                toBeCompensated.add(segment);
                segment = null;
              }
              isContinue = false;
            }
          }
          if (isContinue) {
            toBeCompensated.add(segment);
          }
        } else {
          // 二分
          // 计算中间的key
          // 此处可以考虑用mongo的cursor
          // skip一半的消息数，但由于skip的量过大效率很低，故先判断消息量的数量，如果量过大，
          // 超过阀值，则直接取时间戳的中值，此时我们假设消息的发送速率是均匀的
          Long midId;
          int skip = msgCount >> 1;
          midId = messageDAO.getSkippedMessageId(topic, from, skip);
          logger.debug("Skipped {} msgs of {}", skip, msgCount);
          doCompensation(from, midId, toBeCompensated);
          doCompensation(midId, to, toBeCompensated);
        }
        int increment = sizeOf(toBeCompensated) - compensatedCount;
        logger.info(
            "Total got in segment =" + increment + ",lost=" + lost + "[count=" + msgCount
                + ",topic={},consumer={},zone=" + zone
                + "]", topic, consumer);
      }
    }

    private int sizeOf(ConcurrentLinkedQueue<List<Long>> toBeCompensated) {
      if (toBeCompensated == null) {
        return 0;
      }
      synchronized (toBeCompensated) {
        int size = 0;
        for (List<Long> list : toBeCompensated) {
          size += list.size();
        }
        return size;
      }
    }

    /**
     * 根据下界找上界
     */
    private Long findUntil(Long floor) {
      // 往前推delay秒
      long now = System.currentTimeMillis();
      long delayed = now - (delay * 1000);

      // delay时间点和until后的某时刻，取小值
      long ts = MongoUtil.getTimeMillisByLong(floor);
      ts += maxInterval * 60 * 1000;

      long p = ts < delayed ? ts : delayed;
      logger.debug("delayed {}, until {}, chosen {}", new Object[]{delayed, ts, p});
      // 查出delay前的最后一条消息id
      Long newTil = MongoUtil.BSONTimestampToLong(new BSONTimestamp((int) (p / 1000), 0));
      return newTil;
    }
  }
}

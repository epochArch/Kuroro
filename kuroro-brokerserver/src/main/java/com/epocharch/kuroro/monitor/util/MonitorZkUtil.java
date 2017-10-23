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

package com.epocharch.kuroro.monitor.util;

import com.epocharch.common.constants.ClusterUsage;
import com.epocharch.common.idc.IDCContainer;
import com.epocharch.common.zone.Zone;
import com.epocharch.common.zone.zk.ZkZoneContainer;
import com.epocharch.kuroro.common.constants.Constants;
import com.epocharch.kuroro.common.constants.InternalPropKey;
import com.epocharch.kuroro.common.inner.util.KuroroUtil;
import com.epocharch.zkclient.ZkClient;
import java.util.Set;

public class MonitorZkUtil {

  public static final String localZoneName = getLocalZoneName();
  public static final Zone localZone = getLocalZone();
  public static final String localIDCName = getLocalIDCName();
  public static final String IDCZkKuroroRootPath =
      Constants.ZONE_ZK_PARENT_ROOT + Constants.SEPARATOR + localIDCName + Constants.PRODUCTION;
  public static final String IDCZkTopicPath = IDCZkKuroroRootPath + Constants.ZONE_TOPIC;
  public static final String IDCZkMonitorPath = IDCZkKuroroRootPath + Constants.ZONE_MONITOR;
  public static final String IDCZkBrokerGroupPath = IDCZkKuroroRootPath + Constants.ZONE_BROKER;
  public static final String IDCZkBrokerGroupDefaultPath =
      IDCZkBrokerGroupPath + Constants.SEPARATOR + Constants.KURORO_DEFAULT_BROKER_GROUPNAME;
  public static final String IDCZkConsumerServerDefaultPath =
      IDCZkBrokerGroupDefaultPath + Constants.ZONE_BROKER_CONSUMER;
  public static final String IDCZkProductServerDefaultPath =
      IDCZkBrokerGroupDefaultPath + Constants.ZONE_BROKER_PRODUCER;
  public static final String IDCZkMongoPath = IDCZkKuroroRootPath + Constants.ZONE_MONGO;
  public static final String IDCZkTransferGroupPath =
      IDCZkKuroroRootPath + Constants.IDC_TRANSGROUP;
  public static final String IDCZkTransferConfigPath =
      IDCZkKuroroRootPath + Constants.IDC_TRANS_CONFIG;
  public static final String IDCZkZoneMetaPath = "/ZoneMeta";
  public static final String IDCZkZonesPath = IDCZkZoneMetaPath + Constants.ZONES;
  public static final String IDCZoneZKPath = IDCZkKuroroRootPath + Constants.ZONES;
  public static final String IDCZKRemoteProducerPath =
      IDCZkKuroroRootPath + Constants.ZONE_DATA_EPHE + Constants.SEPARATOR + "RemoteProducer";
  private static final ZkClient _zkClient = ZkZoneContainer.getInstance()
      .getLocalZkClient(ClusterUsage.SOA);

  public static String getLocalZoneName() {
    return ZkZoneContainer.getInstance().getLocalZoneName();
  }

  public static Zone getLocalZone() {
    return ZkZoneContainer.getInstance().getZone(localZoneName);
  }

  public static Zone getZone(String zoneName) {
    return ZkZoneContainer.getInstance().getZone(zoneName);
  }

  public static String getLocalIDCName() {
    String localIDCName = System.getProperty(InternalPropKey.ZONE_IDCNAME);
    if (KuroroUtil.isBlankString(localIDCName)) {
      localIDCName = ZkZoneContainer.getInstance().getIdcContainer().getLocalIdc();
    }
    return localIDCName;
  }

  public static ZkClient getLocalIDCZk() {
    return _zkClient;
  }

  public static ZkClient getIDCZK(String zoneName) {
    ZkClient zkClient = null;
    try {
      zkClient = ZkZoneContainer.getInstance().getZkClient(zoneName, ClusterUsage.SOA);
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    return zkClient;
  }

  public static Set<Zone> getAllZonesInCurrentIdc() {
    Set<Zone> zones = ZkZoneContainer.getInstance().getAllZone();
    for (Zone zone : zones) {
      if (zone.getName().equals(localZoneName)) {
        zones.remove(zone);
      }
    }
    return zones;
  }

  public static Set<String> getAllZoneNameInCurrentIdc() {
    Set<String> zoneNames = ZkZoneContainer.getInstance().getAllZoneName();
    zoneNames.remove(localZoneName);
    return zoneNames;
  }

  public static Set<String> getAllIdcName() {
    IDCContainer idcContainer = ZkZoneContainer.getInstance().getIdcContainer();
    Set<String> idcs = idcContainer.getAllIdc();
    return idcs;
  }
}

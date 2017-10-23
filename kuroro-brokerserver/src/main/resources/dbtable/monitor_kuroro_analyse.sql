
SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for monitor_kuroro_consumer_analyse
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_consumer_analyse`;
CREATE TABLE `monitor_kuroro_consumer_analyse` (
  `Id` bigint(20) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(256) DEFAULT NULL COMMENT 'TOPIC名称',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `OUT_COUNTS` int(11) DEFAULT NULL COMMENT '消费数量',
  `CONSUMER_NAME` varchar(256) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONSUMER_IP` varchar(50) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONN_PORT` int(11) DEFAULT NULL COMMENT '连接端口',
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '开始时间',
  `END_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '结束时间',
  `MEMO` varchar(255) DEFAULT NULL COMMENT '备注信息',
  `ZONE` varchar(20) DEFAULT NULL COMMENT 'ZONENAME',
  PRIMARY KEY (`Id`),
  KEY `INX_START_TIME` (`START_TIME`),
  KEY `inx_topic_name` (`TOPIC_NAME`(128))
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='kuroro分析表';

-- ----------------------------
-- Table structure for monitor_kuroro_consumer_analyse_day
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_consumer_analyse_day`;
CREATE TABLE `monitor_kuroro_consumer_analyse_day` (
  `Id` bigint(20) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(256) DEFAULT NULL COMMENT 'TOPIC name',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `OUT_COUNTS` bigint(20) DEFAULT NULL COMMENT 'out count number',
  `CONSUMER_NAME` varchar(256) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONSUMER_IP` varchar(50) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONN_PORT` int(11) DEFAULT NULL COMMENT 'connection port',
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'start time',
  `END_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'end time',
  `MEMO` varchar(255) DEFAULT NULL COMMENT 'other',
  `ZONE` varchar(20) DEFAULT NULL COMMENT 'ZONENAME',
  PRIMARY KEY (`Id`),
  KEY `INX_START_TIME` (`START_TIME`),
  KEY `inx_topic_name` (`TOPIC_NAME`(128))
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='kuroro consumer day number';

-- ----------------------------
-- Table structure for monitor_kuroro_consumer_analyse_hour
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_consumer_analyse_hour`;
CREATE TABLE `monitor_kuroro_consumer_analyse_hour` (
  `Id` bigint(11) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(256) DEFAULT NULL COMMENT 'TOPIC名称',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `OUT_COUNTS` int(11) DEFAULT NULL COMMENT '消费数量',
  `CONSUMER_NAME` varchar(256) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONSUMER_IP` varchar(50) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONN_PORT` int(11) DEFAULT NULL COMMENT '连接端口',
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `END_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '结束时间',
  `MEMO` varchar(255) DEFAULT NULL COMMENT '备注信息',
  `ZONE` varchar(20) DEFAULT NULL COMMENT 'ZONENAME',
  PRIMARY KEY (`Id`,`START_TIME`),
  KEY `INX_START_TIME` (`START_TIME`),
  KEY `inx_topic_name` (`TOPIC_NAME`(128))
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='kuroro分析表';

-- ----------------------------
-- Table structure for monitor_kuroro_producer_analyse
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_producer_analyse`;
CREATE TABLE `monitor_kuroro_producer_analyse` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(256) DEFAULT NULL COMMENT 'TOPIC名称',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `IN_COUNTS` int(11) DEFAULT NULL COMMENT '生产数量',
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `END_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '结束时间',
  `MEMO` varchar(255) DEFAULT NULL COMMENT '备注信息',
  `ZONE` varchar(20) DEFAULT NULL COMMENT 'ZONENAME',
  PRIMARY KEY (`id`),
  KEY `INX_START_TIME` (`START_TIME`),
  KEY `inx_topic_name` (`TOPIC_NAME`(128))
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='kuroro分析表';

-- ----------------------------
-- Table structure for monitor_kuroro_producer_analyse_day
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_producer_analyse_day`;
CREATE TABLE `monitor_kuroro_producer_analyse_day` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(256) DEFAULT NULL COMMENT 'TOPIC name',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `IN_COUNTS` bigint(20) DEFAULT NULL COMMENT 'producer number',
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'start time',
  `END_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'end time',
  `MEMO` varchar(255) DEFAULT NULL COMMENT 'other',
  `ZONE` varchar(20) DEFAULT NULL COMMENT 'ZONENAME',
  PRIMARY KEY (`id`),
  KEY `INX_START_TIME` (`START_TIME`),
  KEY `inx_topic_name` (`TOPIC_NAME`(128))
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='kuroro producer analyse day';

-- ----------------------------
-- Table structure for monitor_kuroro_producer_analyse_hour
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_producer_analyse_hour`;
CREATE TABLE `monitor_kuroro_producer_analyse_hour` (
  `Id` bigint(20) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(256) DEFAULT NULL COMMENT 'TOPIC名称',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `IN_COUNTS` int(11) DEFAULT NULL COMMENT '生产数量',
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `END_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '结束时间',
  `MEMO` varchar(255) DEFAULT NULL COMMENT '备注信息',
  `ZONE` varchar(20) DEFAULT NULL COMMENT 'ZONENAME',
  PRIMARY KEY (`Id`,`START_TIME`),
  KEY `INX_START_TIME` (`START_TIME`),
  KEY `inx_topic_name` (`TOPIC_NAME`(128))
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='kuroro分析表';

-- ----------------------------
-- Table structure for monitor_operation_log
-- ----------------------------
DROP TABLE IF EXISTS `monitor_operation_log`;
CREATE TABLE `monitor_operation_log` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `ZONE_NAME` varchar(50) DEFAULT NULL,
  `USER_NAME` varchar(50) DEFAULT NULL COMMENT '操作者名字',
  `IP` varchar(16) DEFAULT NULL COMMENT '所在ip地址',
  `MODULE_NAME` varchar(512) DEFAULT NULL COMMENT '操作功能模块名',
  `OPERATION_NAME` varchar(256) DEFAULT NULL COMMENT '操作名',
  `OPERATION_TIME` datetime DEFAULT NULL,
  `OPERATION_DETAILS` longtext COMMENT '操作详细信息',
  `RESULT_CODE` varchar(50) DEFAULT NULL COMMENT '操作结果码(0：成功，1：失败)',
  PRIMARY KEY (`ID`),
  KEY `index_user_name` (`USER_NAME`),
  KEY `index_operation_time` (`OPERATION_TIME`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

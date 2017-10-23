
SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for monitor_kuroro_consumer_analyse
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_consumer_analyse`;
CREATE TABLE `monitor_kuroro_consumer_analyse` (
  `Id` bigint(11) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(50) DEFAULT NULL COMMENT 'TOPIC',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `OUT_COUNTS` int(11) DEFAULT NULL,
  `CONSUMER_NAME` varchar(50) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONSUMER_IP` varchar(50) DEFAULT NULL COMMENT 'CONSUMER_IP',
  `CONN_PORT` int(11) DEFAULT NULL,
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'START_TIME',
  `END_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'START_TIME',
  `MEMO` varchar(255) DEFAULT NULL COMMENT 'START_TIME',
  `ZONE` varchar(30) DEFAULT NULL COMMENT 'ZONENAME',
  PRIMARY KEY (`Id`),
  KEY `INX_START_TIME` (`START_TIME`)
) ENGINE=MEMORY AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='kuroro consumer message 15 min data';

-- ----------------------------
-- Table structure for monitor_kuroro_producer_analyse
-- ----------------------------
DROP TABLE IF EXISTS `monitor_kuroro_producer_analyse`;
CREATE TABLE `monitor_kuroro_producer_analyse` (
  `Id` bigint(11) NOT NULL AUTO_INCREMENT,
  `TOPIC_NAME` varchar(50) DEFAULT NULL COMMENT 'TOPIC',
  `BROKER` varchar(50) DEFAULT NULL COMMENT 'brokerIP',
  `IN_COUNTS` int(11) DEFAULT NULL,
  `START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'START_TIME',
  `END_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'END_TIME',
  `MEMO` varchar(255) DEFAULT NULL COMMENT 'MEMO',
  `ZONE` varchar(30) DEFAULT NULL COMMENT 'zone name',
  PRIMARY KEY (`Id`),
  KEY `INX_START_TIME` (`START_TIME`)
) ENGINE=InnoDB AUTO_INCREMENT=11075963 DEFAULT CHARSET=utf8 COMMENT='kuroro producer message 15 min data';

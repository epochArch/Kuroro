
SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for hdc_menu
-- ----------------------------
DROP TABLE IF EXISTS `hdc_menu`;
CREATE TABLE `hdc_menu` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `pid` int(10) DEFAULT '0' COMMENT '父菜单id',
  `code` varchar(50) NOT NULL COMMENT 'code',
  `name` varchar(100) DEFAULT NULL COMMENT 'name',
  `url` varchar(100) DEFAULT NULL,
  `app` varchar(150) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of hdc_menu
-- ----------------------------
INSERT INTO `hdc_menu` VALUES ('30', '0', 'Kuroro_syscfg', '系统设置', null, 'kuroro');
INSERT INTO `hdc_menu` VALUES ('70', '0', 'Kuroro_appServiceMgr', '应用与服务管理', '', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('130', '0', 'Kuroro', 'Kuroro', null, 'kuroro');
INSERT INTO `hdc_menu` VALUES ('31', '30', 'Kuroro_userMgr', '用户角色管理', 'page/system/userManage', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('32', '30', 'Kuroro_roleMgr', '角色权限管理', 'page/system/roleManage', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1177', '70', 'IDCTransferMgr', 'zone数据同步', 'page/manage/IDCTransferMgr', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('132', '130', 'QueueReg', '消息注册', 'page/kuroro/QueueReg', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('33', '30', 'Kuroro_menuMgr', '菜单管理', 'page/system/menuManage', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('133', '130', 'KuroroChart', '消息报表', 'page/kuroro/KuroroChart', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1154', '30', 'OperationLogQuery', '操作日志查询', 'page/system/OperationLogQuery', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1112', '130', 'KuroroConsumer', '消费者状态', 'page/kuroro/KuroroConsumer', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1113', '130', 'KuroroCompensator', '补偿状态', 'page/kuroro/KuroroCompensator', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1178', '70', 'TopicView', 'Topic查看', 'page/manage/TopicView', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1179', '70', 'QueryMongo', '消息查询', 'page/manage/QueryMongo', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1180', '70', 'MessageTrack', '消息轨迹', 'page/manage/MessageTrack', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1181', '70', 'MessageLog', '消息详情', 'page/manage/MessageLog', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1164', '70', 'DetectorSwitcher', 'Detector开关', 'page/manage/KuroroSwitcher', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1167', '130', 'KuroroHistChart', '历史消息报表', 'page/kuroro/KuroroHistChart', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1168', '130', 'FindMongoData', 'Mongo查询', 'page/kuroro/FindMongoData', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1170', '130', 'BatchTopicMongo', 'BatchTopicToMongo', 'page/kuroro/BatchTopicMongo', 'kuroro');
INSERT INTO `hdc_menu` VALUES ('1176', '130', 'InOutQueueList', '生产与消费量列表', 'page/kuroro/InOutQueueList', 'kuroro');

-- ----------------------------
-- Table structure for hdc_pvg
-- ----------------------------
DROP TABLE IF EXISTS `hdc_pvg`;
CREATE TABLE `hdc_pvg` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `roleId` int(5) NOT NULL COMMENT '角色id',
  `associationId` int(10) NOT NULL,
  `associationType` int(11) NOT NULL COMMENT '1代表菜单权限;2代表数据库权限',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of hdc_pvg
-- ----------------------------
INSERT INTO `hdc_pvg` VALUES ('37', '2', '3', '1');
INSERT INTO `hdc_pvg` VALUES ('38', '2', '10', '1');
INSERT INTO `hdc_pvg` VALUES ('39', '2', '1', '1');
INSERT INTO `hdc_pvg` VALUES ('40', '2', '7', '1');
INSERT INTO `hdc_pvg` VALUES ('41', '2', '6', '1');
INSERT INTO `hdc_pvg` VALUES ('42', '2', '4', '1');
INSERT INTO `hdc_pvg` VALUES ('43', '2', '9', '1');
INSERT INTO `hdc_pvg` VALUES ('44', '2', '11', '1');
INSERT INTO `hdc_pvg` VALUES ('49', '3', '11', '1');
INSERT INTO `hdc_pvg` VALUES ('48', '3', '9', '1');
INSERT INTO `hdc_pvg` VALUES ('1323', '1', '50', '1');
INSERT INTO `hdc_pvg` VALUES ('1324', '1', '53', '1');
INSERT INTO `hdc_pvg` VALUES ('9923', '100', '31', '1');
INSERT INTO `hdc_pvg` VALUES ('9922', '100', '1181', '1');
INSERT INTO `hdc_pvg` VALUES ('9921', '100', '1180', '1');
INSERT INTO `hdc_pvg` VALUES ('9920', '100', '1179', '1');
INSERT INTO `hdc_pvg` VALUES ('9919', '100', '1178', '1');
INSERT INTO `hdc_pvg` VALUES ('9918', '100', '1177', '1');
INSERT INTO `hdc_pvg` VALUES ('9917', '100', '1176', '1');
INSERT INTO `hdc_pvg` VALUES ('9936', '15', '30', '1');
INSERT INTO `hdc_pvg` VALUES ('9935', '15', '1167', '1');
INSERT INTO `hdc_pvg` VALUES ('9924', '100', '30', '1');
INSERT INTO `hdc_pvg` VALUES ('9916', '100', '1112', '1');
INSERT INTO `hdc_pvg` VALUES ('9915', '100', '1113', '1');
INSERT INTO `hdc_pvg` VALUES ('9934', '15', '1181', '1');
INSERT INTO `hdc_pvg` VALUES ('9914', '100', '1170', '1');
INSERT INTO `hdc_pvg` VALUES ('9913', '100', '1168', '1');
INSERT INTO `hdc_pvg` VALUES ('9912', '100', '1167', '1');
INSERT INTO `hdc_pvg` VALUES ('9911', '100', '132', '1');
INSERT INTO `hdc_pvg` VALUES ('9933', '15', '1180', '1');
INSERT INTO `hdc_pvg` VALUES ('9932', '15', '133', '1');
INSERT INTO `hdc_pvg` VALUES ('9931', '15', '1179', '1');
INSERT INTO `hdc_pvg` VALUES ('9910', '100', '1164', '1');
INSERT INTO `hdc_pvg` VALUES ('9909', '100', '133', '1');
INSERT INTO `hdc_pvg` VALUES ('9908', '100', '130', '1');
INSERT INTO `hdc_pvg` VALUES ('9907', '100', '33', '1');
INSERT INTO `hdc_pvg` VALUES ('9930', '15', '130', '1');
INSERT INTO `hdc_pvg` VALUES ('9906', '100', '1154', '1');
INSERT INTO `hdc_pvg` VALUES ('9929', '15', '1178', '1');
INSERT INTO `hdc_pvg` VALUES ('9928', '15', '1176', '1');
INSERT INTO `hdc_pvg` VALUES ('9927', '15', '1112', '1');
INSERT INTO `hdc_pvg` VALUES ('9926', '15', '1154', '1');
INSERT INTO `hdc_pvg` VALUES ('9905', '100', '70', '1');
INSERT INTO `hdc_pvg` VALUES ('9925', '15', '70', '1');
INSERT INTO `hdc_pvg` VALUES ('9904', '100', '32', '1');

-- ----------------------------
-- Table structure for hdc_role
-- ----------------------------
DROP TABLE IF EXISTS `hdc_role`;
CREATE TABLE `hdc_role` (
  `roleCode` varchar(50) CHARACTER SET utf8 NOT NULL,
  `roleName` varchar(100) CHARACTER SET utf8 DEFAULT NULL,
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `appName` varchar(150) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `roleCode` (`roleCode`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of hdc_role
-- ----------------------------
INSERT INTO `hdc_role` VALUES ('qa', 'qa', '2012-09-24 13:27:55', '1', null);
INSERT INTO `hdc_role` VALUES ('admin', 'admin', '2012-09-19 15:07:38', '2', null);
INSERT INTO `hdc_role` VALUES ('sa', 'sa', '2012-09-24 13:28:06', '3', null);
INSERT INTO `hdc_role` VALUES ('kuroro_admin', 'kuroro_admin', '2012-12-24 13:25:41', '100', 'kuroro');
INSERT INTO `hdc_role` VALUES ('kuroro_normal', 'kuroro_normal', '2012-12-24 13:25:41', '15', 'kuroro');

-- ----------------------------
-- Table structure for hdc_user
-- ----------------------------
DROP TABLE IF EXISTS `hdc_user`;
CREATE TABLE `hdc_user` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) DEFAULT NULL COMMENT '用户名',
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of hdc_user
-- ----------------------------

INSERT INTO `hdc_user` VALUES ('138', 'zhoufeiqiang1', '2015-12-07 16:01:00');
-- ----------------------------
-- Table structure for hdc_user_role
-- ----------------------------
DROP TABLE IF EXISTS `hdc_user_role`;
CREATE TABLE `hdc_user_role` (
  `uid` int(10) NOT NULL COMMENT '用户id',
  `roleCodes` varchar(100) NOT NULL,
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of hdc_user_role
-- ----------------------------
INSERT INTO `hdc_user_role` VALUES ('138', 'kuroro_admin', '2015-12-07 16:01:14');

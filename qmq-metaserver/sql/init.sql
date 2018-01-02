CREATE TABLE `broker_group` (
  `id`              INT(11) UNSIGNED NOT NULL AUTO_INCREMENT
  COMMENT '主键',
  `group_name`      VARCHAR(30)      NOT NULL DEFAULT ''
  COMMENT 'group名字',
  `master_address`  VARCHAR(25)      NOT NULL DEFAULT ''
  COMMENT 'master地址,ip:port',
  `slave_info_json` VARCHAR(200)     NOT NULL DEFAULT ''
  COMMENT 'slave信息，json存储',
  `broker_state`    TINYINT          NOT NULL DEFAULT '-1'
  COMMENT 'broker master 状态',
  `create_time`     TIMESTAMP        NOT NULL DEFAULT '1970-01-01 08:00:01'
  COMMENT '创建时间',
  `update_time`     TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_group_name` (`group_name`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = 'broker组信息';


CREATE TABLE `subject_route` (
  `id`                INT(11) UNSIGNED NOT NULL AUTO_INCREMENT
  COMMENT '主键',
  `subject_info`      VARCHAR(100)     NOT NULL DEFAULT ''
  COMMENT '主题',
  `broker_group_json` VARCHAR(300)     NOT NULL DEFAULT ''
  COMMENT 'broker group name信息，json存储',
  `create_time`       TIMESTAMP        NOT NULL DEFAULT '1970-01-01 08:00:01'
  COMMENT '创建时间',
  `update_time`       TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_subject` (`subject_info`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '主题路由信息';



CREATE TABLE ots_user_table (
  table_id BIGSERIAL NOT NULL, --表id
  user_id  INTEGER NOT NULL, --用户id
  tenant_id  INTEGER NOT NULL, --租户id
	table_name VARCHAR(128) not null, --表名
	table_desc VARCHAR(256), --表的描述
	primary_key json not null, --表的主键
	table_columns json not null, --表的列信息
	create_time TIMESTAMP(6) default '2015-01-01 00:00:00'::timestamp without time zone not null, --创建时间
	modify_time TIMESTAMP(6) default '2015-01-01 00:00:00'::timestamp without time zone not null, --最近修改时间
	creator BIGINT not null, --创建人
	modifier BIGINT not null, --最近修改人
-- 	permission BOOLEAN default false not null, --权限信息
-- 	enable BOOLEAN default true not null, --是否启用
  PRIMARY KEY (table_id),
  UNIQUE (user_id, table_name)
);



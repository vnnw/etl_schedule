create external table if not exists ods_mysql.ods_beeper_trans_event__yn_trans_event_vas (
    `id` bigint comment "自增id" ,
    `trans_event_id` bigint comment "运力id" ,
    `service_type` string comment "增值服务类型" ,
    `service_config` string comment "增值服务配置快照" ,
    `is_del` int comment "状态字段" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ) partitioned by(p_day string)
 stored as orc
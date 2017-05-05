create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_contract_renewal_config (
    `trans_task_id` bigint comment "任务ID" ,
    `renewal_status` int comment "是否自动续约, 0:不续约, 1:续约" ,
    `renewal_days` bigint comment "自动续约日期" ,
    `source` int comment "最后一次操作来源" ,
    `operation_user_id` bigint comment "最后一次操作人ID" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ) partitioned by(p_day string)
 stored as orc
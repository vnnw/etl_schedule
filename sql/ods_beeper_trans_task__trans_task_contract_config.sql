create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_contract_config (
    `trans_task_id` bigint comment "任务ID" ,
    `probation_start` date comment "试用开始日期" ,
    `probation_end` date comment "试用结束日期" ,
    `task_duration_days` int comment "任务持续周期: 天" ,
    `created_at` timestamp comment "任务创建时间" ,
    `updated_at` timestamp comment "任务更新时间" ) partitioned by(p_day string)
 stored as orc
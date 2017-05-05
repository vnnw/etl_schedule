create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_log (
    `id` bigint comment "主键ID" ,
    `trans_task_id` bigint comment "任务ID" ,
    `action` string comment "操作" ,
    `before_change` string comment "修改前的任务信息" ,
    `after_change` string comment "修改后的任务信息" ,
    `source` int comment "日志来源" ,
    `operation_user_id` bigint comment "操作人ID" ,
    `operation_user_name` string comment "操作人姓名" ,
    `client_ip` string comment "客户端IP" ,
    `created_at` timestamp comment "创建时间" ) partitioned by(p_day string)
 stored as orc
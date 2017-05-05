create external table if not exists ods_mysql.ods_beeper_trans_match__match_job (
    `id` bigint comment "找司机 JOB ID" ,
    `source_id` bigint comment "需求方id" ,
    `source_type` int comment "需求方类型 100 线路任务" ,
    `status` int comment "状态 100 找司机中 200 找到司机 300 取消找司机 400 未找到司机" ,
    `attempts` bigint comment "重试次数" ,
    `max_attempts` bigint comment "最大重试次数" ,
    `latest_process_id` bigint comment "最新进程ID" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "最后更新时间" ) partitioned by(p_day string)
 stored as orc
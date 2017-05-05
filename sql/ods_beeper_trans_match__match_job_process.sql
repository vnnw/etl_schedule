create external table if not exists ods_mysql.ods_beeper_trans_match__match_job_process (
    `id` bigint comment "找司机进程ID" ,
    `job_id` bigint comment "找司机JOB ID" ,
    `source_id` bigint comment "需求方id" ,
    `source_type` int comment "需求方类型 100 线路任务" ,
    `match_type` int comment "找司机方式 100 竞价方式 200 派单方式" ,
    `status` int comment "状态, 针对不同的找司机方式有不同的状态编码" ,
    `driver_id` bigint comment "找到的司机ID" ,
    `driver_price` bigint comment "司机金额 单位:分" ,
    `customer_price` bigint comment "客户金额 单位:分" ,
    `attempts` bigint comment "该进程是第几次重试" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "最后更新时间" ) partitioned by(p_day string)
 stored as orc
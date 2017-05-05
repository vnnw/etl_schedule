create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_bonus (
    `trans_task_id` bigint comment "线路任务ID" ,
    `distribution_point_startup_count` int comment "保底配送点个数" ,
    `bonus_money` bigint comment "单个配送点提成金额 单位:分" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ) partitioned by(p_day string)
 stored as orc
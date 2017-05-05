create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_distribution_point (
    `id` bigint comment "ID" ,
    `trans_task_id` bigint comment "任务ID" ,
    `contact_name` string comment "联系人姓名" ,
    `contact_mobile` string comment "联系人电话" ,
    `address` string comment "地址" ,
    `seq` int comment "配送点序号" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ,
    `deleted_at` timestamp comment "删除时间" ) partitioned by(p_day string)
 stored as orc
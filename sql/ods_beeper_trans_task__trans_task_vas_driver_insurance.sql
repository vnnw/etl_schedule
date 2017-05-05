create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_vas_driver_insurance (
    `id` bigint comment "线路任务司机保险配置 ID" ,
    `trans_task_id` bigint comment "线路任务ID" ,
    `is_enable` int comment "是否不可用，0可用，1不可用" ,
    `have_driver_insurance` int comment "是否开启司机保障服务: 0关闭，1开启" ,
    `insurance_type` int comment "司机保障类型" ,
    `insurance_begin_at` timestamp comment "保障开启时间" ,
    `price_mode_value` bigint comment "保障收费值" ,
    `price_mode` int comment "收费模式 100: 费率 , 200: 固定费用" ,
    `operator_id` bigint comment "操作人ID" ,
    `source` int comment "操作来源" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ) partitioned by(p_day string)
 stored as orc
create external table if not exists ods_mysql.ods_beeper_trans_event__yn_trans_event_bonus (
    `id` bigint comment "自增id" ,
    `trans_event_id` bigint comment "出车记录id" ,
    `bonus_status` int comment "配送点个数确认状态" ,
    `final_distribution_point_count` int comment "运作主管录入的配送点个数" ,
    `distribution_point_startup_count` int comment "保底配送点个数" ,
    `bonus_money` bigint comment "单个配送点提成金额 单位:分" ,
    `confirm_type` bigint comment "确认类型" ,
    `confirmed_at` timestamp comment "最终确认配送点个数时间，也就是提成入流水的时间" ,
    `created_at` timestamp comment "记录创建时间" ,
    `updated_at` timestamp comment "记录更新时间" ) partitioned by(p_day string)
 stored as orc
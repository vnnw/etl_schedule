create external table if not exists ods_mysql.ods_beeper_trans_event__yn_trans_event_bonus_confirm_log (
    `id` bigint comment "日志id" ,
    `trans_event_id` bigint comment "出车记录id" ,
    `old_final_distribution_point_count` bigint comment "修改前的配送点个数" ,
    `final_distribution_point_count` bigint comment "修改后的配送点个数" ,
    `created_at` timestamp comment "修改时间" ,
    `operation_usr_id` bigint comment "修改人id" ,
    `source` int comment "日志来源" ) partitioned by(p_day string)
 stored as orc
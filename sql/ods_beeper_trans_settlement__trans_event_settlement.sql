create external table if not exists ods_mysql.ods_beeper_trans_settlement__trans_event_settlement (
    `id` bigint comment "唯一主键ID" ,
    `trans_event_id` bigint comment "唯一约束,出车记录id" ,
    `inspect_status` int comment "检察状态" ,
    `is_bonus_charged` int comment "提成是否进流水，1代表进流水，0代表不进流水" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "最后更新时间" ) partitioned by(p_day string)
 stored as orc
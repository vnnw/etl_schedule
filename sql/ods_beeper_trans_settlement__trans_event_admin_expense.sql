create external table if not exists ods_mysql.ods_beeper_trans_settlement__trans_event_admin_expense (
    `id` bigint comment "唯一主键" ,
    `trans_event_id` bigint comment "出车记录id" ,
    `admin_user_id` bigint comment "计费发生的A端用户id" ,
    `type` int comment "价格类型 如：运作主管提成等" ,
    `price` bigint comment "价格（单位：分）" ,
    `status` int comment "结算状态（100:不结算，200:结算，300:取消结算）" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "最后更新时间" ) partitioned by(p_day string)
 stored as orc
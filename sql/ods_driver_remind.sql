create external table if not exists ods.ods_driver_remind (
    id bigint comment "竞价司机岗前提醒 ID" ,
    bid_id bigint comment "司机竞价 ID" ,
    source int comment "调用者来源" ,
    operator_id bigint comment "操作人 ID" ,
    created_at string comment "创建时间" )
 stored as orc
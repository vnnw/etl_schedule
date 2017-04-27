create external table if not exists ods.ods_flbp_logs (
    id bigint comment "" ,
    cuid bigint comment "" ,
    operation string comment "" ,
    delay_day bigint comment "延期天数" ,
    reason_type bigint comment "操作原因类别" ,
    instructions string comment "" ,
    operator string comment "" ,
    opt_time string comment "" ,
    affect_id string comment "影响数据ID" ,
    effect_time string comment "操作生效时间" ,
    created_at string comment "" ,
    updated_at string comment "" )
 stored as orc
create external table if not exists ods.ods_timely_trans_event (
    id bigint comment "主键" ,
    task_id bigint comment "线路任务ID" ,
    task_name string comment "线路任务" ,
    event_id bigint comment "运力ID" ,
    wid bigint comment "仓ID" ,
    cuid bigint comment "所属用户ID" ,
    did bigint comment "司机ID" ,
    work_time double comment "配送日期" ,
    out_later int comment "出仓晚点" ,
    dis_later int comment "配送晚点" ,
    status int comment "状态" ,
    dest_status int comment "配送状态" ,
    check_in_time double comment "签到时间" ,
    complete_time double comment "完成时间" ,
    departure_time double comment "仓签离时间" ,
    config string comment "运力时效配置" ,
    create_time double comment "创建时间" ,
    update_time double comment "修改时间" ,
    config_type int comment "配置类型" ,
    comment string comment "备注" ,
    settle_status int comment "结算状态" ,
    settle_user string comment "结算人" ,
    settle_time bigint comment "结算时间" ,
    check_user string comment "复核人" ,
    check_time bigint comment "复核时间" )
 stored as orc
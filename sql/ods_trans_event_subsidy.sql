create external table if not exists ods.ods_trans_event_subsidy (
    id bigint comment "唯一ID" ,
    trans_event_id bigint comment "运力事件ID" ,
    subsidy_id bigint comment "补贴类型ID" ,
    subsidy_name string comment "补贴名称" ,
    type bigint comment "补贴类型" ,
    subsidy_to bigint comment "该补贴是给谁的 100 标书客户 200 表示司机" ,
    subsidy_mode bigint comment "补贴值类型 100 表示补贴率 200 表示补贴金额" ,
    subsidy_sp double comment "补贴率" ,
    subsidy_money bigint comment "补贴金额" ,
    use_latest bigint comment "是否取最新的补贴规则" ,
    cprice bigint comment "客户补贴金额，单位：分" ,
    dprice bigint comment "司机补贴金额，单位：分" ,
    comment string comment "简要说明" ,
    created_at timestamp comment "创建时间" ,
    updated_at timestamp comment "最后更新时间" )
 stored as orc
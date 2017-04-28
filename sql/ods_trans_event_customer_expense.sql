create external table if not exists ods.ods_trans_event_customer_expense (
    `id`  bigint comment "唯一主键" ,
    `trans_event_id`  bigint comment "出车记录id" ,
    `type`  int comment "价格类型 如：温控服务价格、sop服务价格等" ,
    `price`  bigint comment "价格（单位：分）" ,
    `price_mode`  int comment "价格计算类型：100 按比率计算；200 按固定金额计算" ,
    `mode_number`  double comment "计算价格参照的比率或固定金额（只作为证据，不参加计算）" ,
    `status`  int comment "结算状态（100:不结算，200:结算）" ,
    `inspect_status`  int comment "检察状态" ,
    `updated_at`  timestamp comment "最后更新时间" ,
    `uid`  bigint comment "计费发生的用户id（如：customer_id）" )
 stored as orc
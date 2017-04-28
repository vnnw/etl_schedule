create external table if not exists ods.ods_monitor_result (
    id bigint comment "主键自增ID" ,
    classify int comment "异常分类" ,
    itid bigint comment "派车单号" ,
    warehouse_id bigint comment "仓编号" ,
    deliver_date bigint comment "配送日期" ,
    type int comment "异常类型" ,
    status int comment "异常状态" ,
    deal_result int comment "处理结果" ,
    customer_customer_name string comment "配送点名称" ,
    cuid bigint comment "客户id" ,
    order_id bigint comment "订单编号" ,
    did bigint comment "司机id" ,
    longitude string comment "异常经度" ,
    latitude string comment "异常纬度" ,
    readed int comment "是否已读" ,
    detail string comment "异常详情" ,
    remark string comment "备注" ,
    create_time double comment "创建时间" )
 stored as orc
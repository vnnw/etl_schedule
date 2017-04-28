create external table if not exists ods.ods_itinerary_order (
    id bigint comment "自增id" ,
    cuid bigint comment "派车单id" ,
    it_id bigint comment "派车单id" ,
    order_id bigint comment "订单id" ,
    order_num bigint comment "订单排序" ,
    status int comment "状态值" ,
    create_time bigint comment "创建时间" ,
    update_time bigint comment "更新时间" ,
    from bigint comment "来源" )
 stored as orc
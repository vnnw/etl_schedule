create external table if not exists ods_mysql.ods_beeper2_drivers_driver_item_distribute (
    `distribute_id` bigint comment "司机物品发放信息id" ,
    `driver_id` bigint comment "司机id" ,
    `item_type` int comment "物品类型，1为夏装，2为冬装，3为车贴，4为工牌" ,
    `operate_type` int comment "结果集类型，1为领取，2为更换" ,
    `item_num` bigint comment "物品数量" ,
    `deposit` bigint comment "押金金额，单位分" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" )
 stored as orc
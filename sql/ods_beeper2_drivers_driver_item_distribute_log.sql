create external table if not exists ods_mysql.ods_beeper2_drivers_driver_item_distribute_log (
    `distribute_log_id` bigint comment "司机物品发放记录id" ,
    `driver_id` bigint comment "司机id" ,
    `adcid` bigint comment "司机所属管理区id（仅产生发放记录时，司机可能变更管理区）" ,
    `operate_type` int comment "操作类型，1为领取，2为更换，3为退还" ,
    `item_type` int comment "物品类型，1为夏装，2为冬装，3为车贴，4为工牌" ,
    `item_num` bigint comment "物品数量" ,
    `deposit` bigint comment "押金金额，单位分" ,
    `operator_id` bigint comment "操作人id" ,
    `operated_at` timestamp comment "操作时间" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" )
 stored as orc
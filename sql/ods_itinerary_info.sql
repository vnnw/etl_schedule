create external table if not exists ods.ods_itinerary_info (
    `id`  bigint comment "主键自增ID" ,
    `itid`  bigint comment "派车单ID" ,
    `did`  bigint comment "司机ID" ,
    `depart_time`  bigint comment "离仓时间" ,
    `complete_time`  bigint comment "完成时间" ,
    `mileage`  bigint comment "距离，米" ,
    `jisuan_time`  bigint comment "计算时间" ,
    `status`  int comment "状态" ,
    `create_time`  bigint comment "创建时间" ,
    `update_time`  bigint comment "更新时间" )
 stored as orc
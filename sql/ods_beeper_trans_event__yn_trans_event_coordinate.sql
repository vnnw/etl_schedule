create external table if not exists ods_mysql.ods_beeper_trans_event__yn_trans_event_coordinate (
    `id` bigint comment "经纬度 ID" ,
    `trans_event_id` bigint comment "运力 ID" ,
    `type` int comment "类型, 这里和运力表中 status 取值一致, (400: 签到 800: 离仓 900: 配送完成)" ,
    `longitude` double comment "经度" ,
    `latitude` double comment "纬度" ,
    `coord_sys` int comment "图商(0: 默认值 1: 百度 2: 高德 3: 世界坐标)" ,
    `client_ip` string comment "打点时上报的客户端ip地址" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ,
    `picture_url` string comment "打点类型的图片" ) partitioned by(p_day string)
 stored as orc
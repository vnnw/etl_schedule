create external table if not exists ods_mysql.ods_beeper2_drivers_driver_excellent (
    `id` bigint comment "ID" ,
    `driver_id` bigint comment "司机id" ,
    `score` bigint comment "得分x100" ,
    `stat_month` int comment "统计月份，如201612，表示统计的是2016年12月份的数据" ,
    `is_excellent` int comment "是否为优秀司机,0-不是，1-是" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "" )
 stored as orc
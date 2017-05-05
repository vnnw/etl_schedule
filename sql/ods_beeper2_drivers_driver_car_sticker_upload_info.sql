create external table if not exists ods_mysql.ods_beeper2_drivers_driver_car_sticker_upload_info (
    `id` bigint comment "主键id" ,
    `driver_id` bigint comment "司机id" ,
    `adcid` bigint comment "司机管理区id" ,
    `dd_id` bigint comment "司机拓展id" ,
    `uploaded_at` timestamp comment "上传时间" ,
    `photos` string comment "上传照片" ,
    `status` int comment "检查状态" ,
    `reason` string comment "不合格原因" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" )
 stored as orc
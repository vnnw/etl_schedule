create external table if not exists ods_mysql.ods_beeper2_drivers_driver_history_distribution (
    `id` bigint comment "主键" ,
    `driver_id` bigint comment "司机id" ,
    `customer_id` bigint comment "客户id" ,
    `contact_name` string comment "收货人" ,
    `contact_mobile` string comment "收货人联系方式" ,
    `address` string comment "送货地址" ,
    `is_delete` int comment "该记录是否被删除" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" )
 stored as orc
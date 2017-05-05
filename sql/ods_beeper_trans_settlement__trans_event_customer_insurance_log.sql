create external table if not exists ods_mysql.ods_beeper_trans_settlement__trans_event_customer_insurance_log (
    `id` bigint comment "唯一id" ,
    `trans_event_id` bigint comment "出车记录编号" ,
    `yn_insurance_price` bigint comment "云鸟给保险公司的保险费，单位:分" ,
    `is_for_test` int comment "是否是测试数据，1代表哥谭市测试数据，0代表其他城市数据" ,
    `status` int comment "保险确认状态（调保险公司api的返回code）" ,
    `params` string comment "调第三方接口传的参数" ,
    `response` string comment "调第三方接口返回的结果" ,
    `response_at` timestamp comment "第三方api结果返回时间" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "最后一次更新时间" ) partitioned by(p_day string)
 stored as orc
create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_contract_renewal_log (
    `id` bigint comment "主键ID" ,
    `trans_task_id` bigint comment "线路任务ID" ,
    `renew` int comment "是否续约成功,0:未续约,1:已续约" ,
    `renewal_days` bigint comment "续约日期" ,
    `end_date_before` date comment "续约前的任务到期日期" ,
    `end_date_after` date comment "续约后的任务到期日期" ,
    `created_at` timestamp comment "续约时间" ,
    `updated_at` timestamp comment "续约时间" ) partitioned by(p_day string)
 stored as orc
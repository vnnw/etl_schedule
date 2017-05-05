create external table if not exists ods_mysql.ods_beeper_trans_task__trans_task_vas_sop (
    `id` bigint comment "线路任务 SOP 配置 ID" ,
    `trans_task_id` bigint comment "线路任务ID" ,
    `customer_id` bigint comment "客户ID" ,
    `warehouse_id` bigint comment "仓ID" ,
    `sop_mgr_id` bigint comment "运作经理ID" ,
    `fcc_id` bigint comment "运作主管ID" ,
    `is_enable` int comment "当前配置是否生效" ,
    `have_sop` int comment "是否开启SOP服务, 0: 关闭, 1: 开启" ,
    `sop_rate` double comment "SOP服务费率" ,
    `sop_prejob_dates` string comment "SOP岗前培训日期, 以逗号分隔的多个日期字符串，eg:2015-11-04,2015-11-05" ,
    `sop_prejob_dates_msg` string comment "SOP岗前培训日期说明, eg:无SOP岗前培训安排" ,
    `begin_date` date comment "SOP服务生效时间" ,
    `end_date` date comment "SOP服务生效结束时间" ,
    `operator_id` bigint comment "操作人ID" ,
    `source` int comment "操作来源" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ) partitioned by(p_day string)
 stored as orc
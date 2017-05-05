create external table if not exists ods_mysql.ods_beeper_trans_event__yn_trans_event_log (
    `id` bigint comment "日志ID" ,
    `trans_event_id` bigint comment "运力事件id" ,
    `source` int comment "日志来源" ,
    `action` string comment "操作" ,
    `operation_time` timestamp comment "发生时间" ,
    `operation_user_id` bigint comment "操作人ID" ,
    `comment` string comment "说明" ,
    `customer_id` bigint comment "运力事件所属客户id" ,
    `warehouse_id` bigint comment "运力事件所属仓库id" ,
    `driver_id` bigint comment "运力事件所属司机id" ,
    `trans_task_id` bigint comment "运力事件所属线路任务id" ,
    `line_name` string comment "运力事件所属线路任务的线路名称" ,
    `work_time` timestamp comment "司机应该上班的时间" ,
    `adc_id` bigint comment "司机签到时刻的拓展经理所属管理区id的快照" ,
    `customer_adc_id` bigint comment "客户所属管理区，与运力和任务上记录的adc_id一致" ) partitioned by(p_day string)
 stored as orc
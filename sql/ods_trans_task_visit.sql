create external table if not exists ods.ods_trans_task_visit (
    id bigint comment "visit id" ,
    trans_task_id bigint comment "任务id" ,
    driver_id bigint comment "司机id" ,
    created_at timestamp comment "创建时间" )
 stored as orc
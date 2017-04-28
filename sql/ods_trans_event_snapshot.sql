create external table if not exists ods.ods_trans_event_snapshot (
    trans_event_id bigint comment "运力id" ,
    project_type int comment "客户项目类型" ,
    bu_leader_id bigint comment "客户所属bu leader的id" )
 stored as orc
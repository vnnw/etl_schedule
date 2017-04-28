create external table if not exists ods.ods_accident_extra_info (
    `id` bigint comment "" ,
    `accident_id` bigint comment "事故ID" ,
    `customer_price_per_day` bigint comment "客户基础运力费，单位：分" ,
    `driver_price_per_day` bigint comment "司机基础运力费，单位：分" ,
    `adc_id` bigint comment "客户所属区域ID" ,
    `sales_id` bigint comment "客户顾问ID" ,
    `bid_mgr_id` bigint comment "岗位经理ID" ,
    `fcc_id` bigint comment "运作主管ID" ,
    `fcc_mgr_id` bigint comment "运作经理ID" ,
    `dd_id` bigint comment "拓展经理ID" ,
    `task_id` bigint comment "线路任务ID" ,
    `event_id` bigint comment "运力ID" ,
    `warehouse_id` bigint comment "仓库ID" ,
    `create_time` bigint comment "创建时间" ,
    `update_time` bigint comment "更新时间" ,
    `cargo_insurance_price` bigint comment "客户保价费(含税) 单位：分" ,
    `driver_insurance_money` bigint comment "司机保障费 单位：分" )
 stored as orc
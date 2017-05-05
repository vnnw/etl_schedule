create external table if not exists ods_mysql.ods_beeper_trans_event__yn_trans_event_price_snapshot (
    `id` bigint comment "自增id" ,
    `trans_event_id` bigint comment "运力id" ,
    `invoice_tax_rate` double comment "存储当时需开发票的税率" ,
    `customer_tcsp` double comment "客户运力补贴率 transport capacity subsidy percentage  例如20%，tcsp=0.2" ,
    `finance_pay_type` int comment "客户类型（客户的财务支付类型）" ,
    `first_onboard_rate` double comment "首岗奖费率 例如: 0.20 (20%)" ,
    `sop_royalty_rate` double comment "现控员提成率（签到时对应的提成率）" ,
    `subsidy_content` string comment "json，补贴数组，每个元素是一个补贴对象" ,
    `created_at` timestamp comment "创建时间" ,
    `updated_at` timestamp comment "更新时间" ) partitioned by(p_day string)
 stored as orc
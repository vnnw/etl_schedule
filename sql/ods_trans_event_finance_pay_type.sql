create external table if not exists ods.ods_trans_event_finance_pay_type (
    trans_event_id bigint comment "运力id" ,
    finance_pay_type int comment "客户类型（客户的财务支付类型）" )
 stored as orc
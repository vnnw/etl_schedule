create external table if not exists ods_mysql.ods_beeper_customer_api_customer_group (
    `id` bigint comment "集团id" ,
    `name` string comment "集团名称" ,
    `admin_account_id` bigint comment "集团管理员账号id" ,
    `contact_name` string comment "联系人姓名" ,
    `contact_title` string comment "联系人职位" ,
    `contact_telephone` string comment "联系人电话" ,
    `comment` string comment "备注" ,
    `sales_id` bigint comment "销售负责人A端id" ,
    `creator_id` bigint comment "创建人A端id" ,
    `status` int comment "集团状态" ,
    `created_at` timestamp comment "" ,
    `updated_at` timestamp comment "" )
 stored as orc
with source as (select * from {{ source('ELICENSE_RAW', 'ACCOUNT') }}),

renamed as (
    select
        id as account_id,
        name as account_name,
        doing_business_as_1__c as doing_business_as_1,
        doing_business_as_all_3__c as doing_business_as_all_3,
        row_number() over (partition by id order by lastmodifieddate desc) as rn
    from
        source
)

select
    account_id,
    account_name,
    doing_business_as_1,
    doing_business_as_all_3
from
    renamed
where rn = 1

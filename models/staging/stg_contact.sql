with source as (
    select *
    from
        {{ source('ELICENSE_RAW', 'CONTACT') }}
),

renamed as (
    select
        id as contact_id,
        lastname,
        firstname,
        middlename,
        contact_suffix__c as suffix,
        ROW_NUMBER() over (
            partition by id
            order by lastmodifieddate desc
        ) as rn
    from
        source
)

select
    contact_id,
    lastname,
    firstname,
    middlename,
    suffix
from
    renamed
where rn = 1

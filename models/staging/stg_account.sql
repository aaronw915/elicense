WITH source AS (SELECT * FROM {{ source('ELICENSE_RAW', 'ACCOUNT') }}),

renamed AS (
    SELECT
        id AS account_id,
        name AS account_name,
        {{ normalize_csv_text('doing_business_as_1__c') }} AS doing_business_as_1,
        {{ normalize_csv_text('doing_business_as_all_3__c') }} AS doing_business_as_all_3,
        row_number() OVER (PARTITION BY id ORDER BY lastmodifieddate DESC) AS rn
    FROM
        source
)

SELECT
    account_id,
    account_name,
    doing_business_as_1,
    doing_business_as_all_3
FROM
    renamed
WHERE rn = 1

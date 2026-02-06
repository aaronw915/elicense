SELECT
    UPPER(trim(
        concat(
            b.licensee_name,
            ' ',
            coalesce(a.doing_business_as_1, '')
        )
    )) AS business_name,
    board_name,
    license_number,
    type,
    status,
    sub_status,
    sub_category,
    board_action,
    parcel_street_address,
    parcel_city,
    parcel_state,
    parcel_county,
    parcel_country,
    license_issue_date,
    license_effective_date,
    license_expiration_date
FROM
    {{ref('dim_license_business')}} b
LEFT JOIN {{ref('stg_account')}} a ON b.account_id = a.account_id
LEFT JOIN {{ref('stg_addresses')}} c ON b.license_id = c.license_id
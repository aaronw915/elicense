WITH numbered AS (
    SELECT
        specialty_license_id,
        med_specialty_type,
        med_specialty,
        med_sub_specialty,
        national_certification_organization,
        national_certification_expiration_date,
        national_certification_specialty,
        national_certification_foci,

        ROW_NUMBER() OVER (
            PARTITION BY specialty_license_id
            ORDER BY
                med_specialty_type,
                med_specialty,
                med_sub_specialty,
                national_certification_organization,
                national_certification_expiration_date,
                national_certification_specialty,
                national_certification_foci
        ) AS seq
    FROM {{ ref('stg_specialty') }}
),

pivoted AS (
    SELECT
        specialty_license_id,
        COUNT(*) AS specialty_count,

        {% for i in range(1, 11) %}
            MAX(CASE WHEN seq = {{ i }} THEN med_specialty END)
                AS med_specialty_{{ i }},
            MAX(CASE WHEN seq = {{ i }} THEN med_sub_specialty END)
                AS med_sub_specialty_{{ i }},
            MAX(CASE WHEN seq = {{ i }} THEN national_certification_organization END)
                AS national_cert_organization_{{ i }},
            MAX(CASE WHEN seq = {{ i }} THEN national_certification_expiration_date END)
                AS national_cert_expiration_date_{{ i }}{% if not loop.last %},{% endif %}
        {% endfor %}

    FROM numbered
    GROUP BY specialty_license_id
)

SELECT *
FROM pivoted
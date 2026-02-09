WITH numbered AS (
    SELECT DISTINCT
        endorsement_license_id,
        CASE
            WHEN endorsement_number ILIKE '%CTR%'
                THEN NULL
            WHEN endorsement_number ILIKE '%(RX)%'
                THEN NULL
            ELSE endorsement_number
        END AS endorsement_number,
        endorsement_type,
        endorsement_status,
        endorsement_sub_status,
        endorsement_sub_category,
        issue_date,
        expiration_date,
        row_number() OVER (
            PARTITION BY endorsement_license_id
            ORDER BY issue_date DESC, endorsement_number DESC
        ) AS seq
    FROM {{ ref('stg_endorsement') }}
),

pivoted AS (
    SELECT
        endorsement_license_id,
        count(*) AS endorsement_count,

        {% for i in range(1, 11) %}
            max(CASE WHEN seq = {{ i }} THEN endorsement_number END)
                AS endorsement_number_{{ i }},
            max(CASE WHEN seq = {{ i }} THEN endorsement_type END)
                AS endorsement_type_{{ i }},
            max(CASE WHEN seq = {{ i }} THEN endorsement_status END)
                AS endorsement_status_{{ i }},
            max(CASE WHEN seq = {{ i }} THEN endorsement_sub_status END)
                AS endorsement_sub_status_{{ i }},
            max(CASE WHEN seq = {{ i }} THEN endorsement_sub_category END)
                AS endorsement_sub_category_{{ i }},
            max(CASE WHEN seq = {{ i }} THEN issue_date END)
                AS endorsement_issue_date_{{ i }},
            max(CASE WHEN seq = {{ i }} THEN expiration_date END)
                AS endorsement_expiration_date_{{ i }},
        {% endfor %}

    FROM numbered
    GROUP BY endorsement_license_id
)

SELECT *
FROM pivoted

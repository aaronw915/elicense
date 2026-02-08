WITH numbered AS (
    SELECT DISTINCT
        endorsement_license_id,
        endorsement_number,
        endorsement_type,
        endorsement_status,
        issue_date,
        expiration_date,
        row_number() OVER (
            PARTITION BY endorsement_license_id
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
            max(CASE WHEN seq = {{ i }} THEN issue_date END)
                AS issue_date_{{ i }},
            max(CASE WHEN seq = {{ i }} THEN expiration_date END)
                AS expiration_date_{{ i }},
        {% endfor %}

    FROM numbered
    GROUP BY endorsement_license_id
)

SELECT *
FROM pivoted

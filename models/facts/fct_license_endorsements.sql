with numbered as (
    select
        endorsement_license_id,
        endorsement_number,
        endorsement_type,
        endorsement_status,
        issue_date,
        expiration_date,
        row_number() over (
            partition by endorsement_license_id
            order by endorsement_id
        ) as seq
    from {{ ref('stg_endorsement') }}
),

pivoted as (
    select
        endorsement_license_id,
        count(*) as endorsement_count,

        {% for i in range(1, 11) %}
            max(case when seq = {{ i }} then endorsement_number end)
                as endorsement_number_{{ i }},
            max(case when seq = {{ i }} then endorsement_type end)
                as endorsement_type_{{ i }},
            max(case when seq = {{ i }} then endorsement_status end)
                as endorsement_status_{{ i }},
            max(case when seq = {{ i }} then issue_date end)
                as issue_date_{{ i }},
            max(case when seq = {{ i }} then expiration_date end)
                as expiration_date_{{ i }},
        {% endfor %}

    from numbered
    group by endorsement_license_id
)

select *
from pivoted

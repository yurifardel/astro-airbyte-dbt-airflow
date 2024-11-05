with source_cte_proposta as (
    select
        *
    from 
        {{ref('cte_proposta')}}
)
select
    *
from
    source_cte_proposta p
with source_proposta as (
    select 
        *
    from
        {{source('silver', 'proposta')}}
)
select 
	  sgpp.id as id_proposta
	, sgpp."origemComercial" as id_origem_comercial
	, sgpp.produto as id_produto
	, sgpp.usuario as id_usuario
	, trim(sgpp.status) as ds_status
	, sgpp.fluxo as cod_fluxo
	, sgpp.data as dt_cadastro
	, sgpp."dataDecisao" as dt_decisao
	, sgpp."reenviosSmsTac" as qtd_reenvios_sms_tac
	, sgpp.marcadores as nu_marcadores
from    
    source_proposta sgpp
order by sgpp.id
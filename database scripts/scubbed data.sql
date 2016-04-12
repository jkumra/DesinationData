--select *, case when category_ids like '%361%' then 'Found' else 'NA' end from factual_Destinations 
--where category_ids like '%361%' 
--limit 10


select a.* , b.searchable,b.claimable from factual_Destinations a join (
select factual_id, case when (sum(case when searchable=True then 1 else 0 end )) > 0 then 'Yes' else 'No' end as searchable, 
Case when(sum(case when claimable =True then 1 else 0 end )) > 0 then 'Yes' else 'No' end as claimable  
from factual_Destinations a join factual_category_inclusion_list b on replace(replace(a.category_ids,']',''),'[','') like '%'||b.category_id::text||'%' 
where Upper(locality)='GREENVILLE' and upper(region)='SC' and factual_id not in (select duplicate_factual_id from factual_duplicates where duplicate_check_type=1)
group by factual_id) b on a.factual_id=b.factual_id
where Upper(locality)='GREENVILLE' and upper(region)='SC' 
--b on a.factual_id=b.factual_id  


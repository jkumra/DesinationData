
create table factual_cities as
select ROW_NUMBER() Over () as id, region,locality from (select distinct LOCALITY,REGION  from factual_destinations order by region,locality) a

--Question 1
--Order by N/A

with cte1 as (
select  


cm.continent_code
,co.continent_name
,IFNULL(cu.country_code,'N/A') as country_code
,IFNULL(cu.country_name, 'N/A') as country_name
,pc.year
,pc.gdp_per_capita
,IF(cu.country_code is null , 1,0) as scoring


from spheric-crow-425016-d7.zm_data.continent_map cm
left join spheric-crow-425016-d7.zm_data.continents as co
  on co.continent_code = cm.continent_code
left join  spheric-crow-425016-d7.zm_data.countries as cu
  on cu.country_code = cm.country_code
left join `spheric-crow-425016-d7.zm_data.per_capita` as pc
  on pc.country_code = cu.country_code

)

--Question 2
--GDP Groth YoY

with cte1 as (
select  


cm.continent_code
,co.continent_name
,cu.country_code
,IFNULL(cu.country_name, 'N/A') as country_name
,pc.year
,pc.gdp_per_capita
,lag(gdp_per_capita) OVER (PARTITION BY country_name order by year) as previous_month


from spheric-crow-425016-d7.zm_data.continent_map cm
left join spheric-crow-425016-d7.zm_data.continents as co
  on co.continent_code = cm.continent_code
left join  spheric-crow-425016-d7.zm_data.countries as cu
  on cu.country_code = cm.country_code
left join `spheric-crow-425016-d7.zm_data.per_capita` as pc
  on pc.country_code = cu.country_code

order by 4
),
scoring as (

select
*
,nullif(cte1.gdp_per_capita,0)-nullif(previous_month,0) as growth_dollars
,(nullif(cte1.gdp_per_capita,0)-nullif(previous_month,0)) / NULLIF(previous_month,0) as gdp_per_capita_growth_perc
,ROW_NUMBER () OVER (PARTITION BY year order by (nullif(cte1.gdp_per_capita,0)-nullif(previous_month,0)) / NULLIF(previous_month,0) desc ) as rn
from cte1
where year = '2012'
)

select  

 rn as Ranking
,country_name
,country_code
,continent_name as continent
,scoring.gdp_per_capita_growth_perc

from scoring
where rn <= 10




--NA / Eur/ Rest of World
--Question 3

with cte1 as (
select  


cm.continent_code
,co.continent_name
,cu.country_code
,IFNULL(cu.country_name, 'N/A') as country_name
,pc.year
,pc.gdp_per_capita
,lag(gdp_per_capita) OVER (PARTITION BY country_name order by year) as previous_month


from spheric-crow-425016-d7.zm_data.continent_map cm
left join spheric-crow-425016-d7.zm_data.continents as co
  on co.continent_code = cm.continent_code
left join  spheric-crow-425016-d7.zm_data.countries as cu
  on cu.country_code = cm.country_code
left join `spheric-crow-425016-d7.zm_data.per_capita` as pc
  on pc.country_code = cu.country_code

order by 4
),
scoring as (

select

case when continent_name not in ('North America', 'Europe') then 'Rest of the World' else continent_name end as continent_name
,sum(gdp_per_capita) as gdp_per_capita
from cte1
where year = '2012'
group by 1
)

select  

 continent_name,
    gdp_per_capita,
    gdp_per_capita / (SELECT SUM(gdp_per_capita) FROM scoring) AS gdp_per_capita_ratio
    
    from scoring


-- Average GDP 
-- Question 4

with cte1 as (
select  


cm.continent_code
,co.continent_name
,cu.country_code
,IFNULL(cu.country_name, 'N/A') as country_name
,pc.year
,pc.gdp_per_capita
,lag(gdp_per_capita) OVER (PARTITION BY country_name order by year) as previous_month


from spheric-crow-425016-d7.zm_data.continent_map cm
left join spheric-crow-425016-d7.zm_data.continents as co
  on co.continent_code = cm.continent_code
left join  spheric-crow-425016-d7.zm_data.countries as cu
  on cu.country_code = cm.country_code
left join `spheric-crow-425016-d7.zm_data.per_capita` as pc
  on pc.country_code = cu.country_code

order by 4
)


select

year
,continent_name
,AVG(gdp_per_capita) as average_gdp
from cte1
where year is not null
group by 1,2


--Question 5
--Median GDP
with cte1 as (
select  


cm.continent_code
,co.continent_name
,cu.country_code
,IFNULL(cu.country_name, 'N/A') as country_name
,pc.year
,pc.gdp_per_capita
,lag(gdp_per_capita) OVER (PARTITION BY country_name order by year) as previous_month


from spheric-crow-425016-d7.zm_data.continent_map cm
left join spheric-crow-425016-d7.zm_data.continents as co
  on co.continent_code = cm.continent_code
left join  spheric-crow-425016-d7.zm_data.countries as cu
  on cu.country_code = cm.country_code
left join `spheric-crow-425016-d7.zm_data.per_capita` as pc
  on pc.country_code = cu.country_code

order by 4
),
gdp_data AS (
  SELECT year, continent_name, gdp_per_capita
  FROM cte1
),
ranked_data AS (
  SELECT
    year,
    continent_name,
    gdp_per_capita,
    ROW_NUMBER() OVER (PARTITION BY year, continent_name ORDER BY gdp_per_capita) AS row_num,
    COUNT(*) OVER (PARTITION BY year, continent_name) AS total_rows
  FROM gdp_data
)
SELECT
  year,
  continent_name,
  AVG(gdp_per_capita) AS median_gdp_per_capita
FROM (
  SELECT
    year,
    continent_name,
    gdp_per_capita
  FROM
    ranked_data
  WHERE
    row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2))
)
where year is not null
GROUP BY year, continent_name
ORDER BY year, continent_name;

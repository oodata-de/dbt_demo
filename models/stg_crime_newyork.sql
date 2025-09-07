WITH CTE AS (
    select *
    from {{ source('uscrime', 'URBAN_CRIME_INCIDENT_LOG') }}
    where CITY = 'New York' and OFFENSE_CATEGORY in ('Theft', 'Driving Under The Influence')
)

select
    OFFENSE_CATEGORY,
    {{ get_season('DATE') }} AS SEASON,
    {{ day_type('DATE') }} AS DAY_TYPE,
    count(OFFENSE_CATEGORY) AS NUMBER_OFFENCE
from CTE
group by OFFENSE_CATEGORY, SEASON, DAY_TYPE
order by NUMBER_OFFENCE desc
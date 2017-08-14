-- Step 2. populate #ValidOutcomes with 'true' LB pregnancy outcome dates

with cteTargetPeople (person_id) as
(
  select distinct person_id
  from #pregnancy_events pe
  where pe.category = 'LB'
),
cteTargetCategory (category) as
(
  select first_preg_category from @resultsDatabaseSchema.outcome_limit
  UNION
  select outcome_preg_category from @resultsDatabaseSchema.outcome_limit
)
SELECT pe.person_id, min(pe.EVENT_ID) as event_id
INTO #PregnancyEvents
from #pregnancy_events pe
join cteTargetPeople tp on pe.person_id = tp.person_id
join cteTargetCategory tc on tc.category = pe.category
group by pe.person_id, pe.category, pe.event_date, pe.gest_value
;

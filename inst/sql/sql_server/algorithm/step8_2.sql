select distinct a.person_id, a.event_id
into #FirstOutcomeEventInv
from @resultsDatabaseSchema.FirstOutcomeEvent a
JOIN #pregnancy_events foe on foe.person_id = a.person_id and foe.EVENT_ID = a.EVENT_ID
JOIN #pregnancy_events sp on sp.person_id=a.person_id
where sp.category in ('AGP', 'PCONF') and datediff(dd,foe.EVENT_DATE,sp.event_date)>0
	and datediff(dd,foe.EVENT_DATE,sp.event_date)<=42 ;

with ctePriorOutcomes as (
	select pe.person_id, pe.event_id,
		case when pe.event_date <= foe.event_date then 1 else 0 end as prior
	FROM #ValidOutcomes e
	JOIN #pregnancy_events pe on pe.EVENT_ID = e.EVENT_ID and pe.person_id = e.person_id
	JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo on fo.PERSON_ID = pe.PERSON_ID
	JOIN #pregnancy_events foe on foe.person_id = fo.person_id and foe.EVENT_ID = fo.EVENT_ID
),
cteInvalidOutcomes as
(
	select fo.person_id, fo.event_id
	FROM #ValidOutcomes e
	JOIN #pregnancy_events pe on pe.EVENT_ID = e.EVENT_ID and pe.person_id = e.person_id
	JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo on fo.PERSON_ID = pe.PERSON_ID
	JOIN #pregnancy_events foe on foe.EVENT_ID = fo.EVENT_ID and foe.person_id = fo.person_id
	JOIN ctePriorOutcomes po on po.event_id=pe.event_id and po.person_id = pe.person_id
	JOIN @resultsDatabaseSchema.outcome_limit o1 on o1.FIRST_PREG_CATEGORY = foe.Category AND o1.OUTCOME_PREG_CATEGORY = pe.Category
	JOIN @resultsDatabaseSchema.outcome_limit o2 on o2.FIRST_PREG_CATEGORY = pe.Category AND o2.OUTCOME_PREG_CATEGORY = foe.Category
	WHERE (abs(datediff(d,foe.EVENT_DATE, pe.EVENT_DATE) + 1) < o2.MIN_DAYS and prior=1)
	  or (abs(datediff(d,foe.EVENT_DATE, pe.EVENT_DATE) + 1) < o1.MIN_DAYS and prior=0)
)
select a.person_id, a.event_id
INTO #temp_ValidOutcomes
from @resultsDatabaseSchema.FirstOutcomeEvent a
left join cteInvalidOutcomes b on a.person_id = b.person_id and a.event_id=b.event_id
left join #FirstOutcomeEventInv c on a.person_id = c.person_id and a.event_id=c.EVENT_ID
where b.event_id is null and c.EVENT_ID is null;

INSERT INTO #ValidOutcomes (PERSON_ID, EVENT_ID)
select PERSON_ID, EVENT_ID from #temp_ValidOutcomes;

DROP TABLE #temp_ValidOutcomes;

with cteTargetPeople (person_id) as
(
  select distinct e.person_id
  from #PregnancyEvents e
  join #pregnancy_events pe on e.person_id = pe.person_id and e.event_id = pe.event_id
  where pe.category = 'DELIV'
)
select pe.PERSON_ID, pe.EVENT_ID
INTO #temp_PregnancyEvents
FROM #PregnancyEvents pe
join cteTargetPeople p on pe.person_id = p.person_id
left join @resultsDatabaseSchema.FirstOutcomeEvent de on pe.person_id = de.person_id and pe.event_id = de.event_id
where de.person_id is null;

TRUNCATE TABLE #PregnancyEvents;
DROP TABLE #PregnancyEvents;

SELECT PERSON_ID, EVENT_ID
INTO #PregnancyEvents
from #temp_PregnancyEvents
;

TRUNCATE TABLE #temp_PregnancyEvents;
DROP TABLE #temp_PregnancyEvents;

TRUNCATE TABLE @resultsDatabaseSchema.FirstOutcomeEvent;
DROP TABLE @resultsDatabaseSchema.FirstOutcomeEvent;

TRUNCATE TABLE #FirstOutcomeEventInv;
DROP TABLE #FirstOutcomeEventInv;

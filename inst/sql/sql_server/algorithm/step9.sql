-- Step 9. Find Pregnancy Starts based on the outcome category.


with cteOutcomeEvents (PERSON_ID, EVENT_ID, CATEGORY, gest_value, EVENT_DATE) as
(
	select pe.PERSON_ID, pe.EVENT_ID, pe.Category, pe.gest_value, pe.EVENT_DATE
	FROM #pregnancy_events pe
    JOIN #ValidOutcomes o on pe.PERSON_ID = o.PERSON_ID and pe.EVENT_ID = o.EVENT_ID
),
ctePriorOutcomeDates (PERSON_ID, EVENT_ID, PRIOR_OUTCOME_DATE, prior_category) as
(
	select PERSON_ID, EVENT_ID, PRIOR_OUTCOME_DATE, prior_category
	FROM
	(
		select o.person_id, o.EVENT_ID, p.EVENT_DATE as PRIOR_OUTCOME_DATE, p.category as prior_category,
		row_number() over (partition by o.person_id, o.EVENT_ID order by p.event_date desc) as rn
		from cteOutcomeEvents o
		left join cteOutcomeEvents p on p.PERSON_ID = o.PERSON_ID and o.EVENT_DATE > p.EVENT_DATE
	) E
	where E.rn = 1
),
cteLMPStartDates (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, EPISODE_START_DATE, Category, 1 as DATE_RANK
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, cast(p.EVENT_DATE as date) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE desc) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.person_id = e.person_id and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		WHERE p.Category = 'LMP'
			and p.EVENT_DATE between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn = 1
),
cteGestStartDates (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, 2 as DATE_RANK
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, dateadd(d,(-1 * p.gest_value) + 1, p.EVENT_DATE) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE desc) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.person_id = e.person_id and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		where p.CATEGORY = 'GEST'
			and dateadd(d,(-1 * p.gest_value) + 1, p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn=1
),
cteOvulStartDates (person_id, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, 3 as DATE_RANK
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, dateadd(d,(-14) + 1, p.EVENT_DATE) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		where p.CATEGORY = 'OVUL'
			and dateadd(d,(-14) + 1, p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn=1
),
cteOvul2StartDates (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, 4 as DATE_RANK
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, dateadd(d,(-14) + 1, p.EVENT_DATE) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = pod.EVENT_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		where p.CATEGORY = 'OVUL2'
			and dateadd(d,(-14) + 1, p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn=1
),
cteNuchalUltrasoundStartDates(PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select Q.PERSON_ID, Q.EVENT_ID, Q.EPISODE_START_DATE, Q.CATEGORY, 6 as DATE_RANK
	from
	(
		select e.person_id, e.EVENT_ID as EVENT_ID, dateadd(d,-89,p.EVENT_DATE) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE asc) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		WHERE p.Category = 'NULS'
			and dateadd(d,-89,p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where Q.rn = 1
),
cteAFPStartDates(PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select Q.PERSON_ID, Q.EVENT_ID, Q.EPISODE_START_DATE, Q.CATEGORY, 7 as DATE_RANK
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, dateadd(d,-123,p.EVENT_DATE) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE asc) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		WHERE p.Category = 'AFP'
			and dateadd(d,-123,p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where Q.rn = 1
),
cteAMENStartDates (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, 80 as DATE_RANK
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, dateadd(d,(-56) + 1, p.EVENT_DATE) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		where p.CATEGORY = 'AMEN'
			and dateadd(d,(-56) + 1, p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn=1
),
ctePOptumStartDates (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, 90 as DATE_RANK
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, dateadd(d,(-56) + 1, p.EVENT_DATE) as EPISODE_START_DATE, p.Category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		where p.CATEGORY in  ('UP')
			and dateadd(d,(-56) + 1, p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn=1
),
ctePCONFStartDates (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY) as
(
	select PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, dateadd(d,(-56) + 1, p.EVENT_DATE) as EPISODE_START_DATE,
		  'PUSHBACK' as category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE asc) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		WHERE p.Category in  ('PCONF','AGP','PCOMP', 'TA')
			and dateadd(d,(-56) + 1, p.EVENT_DATE) between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn = 1
),
cteCONTRAStartDates (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY) as
(
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY
	from
	(
		select e.PERSON_ID, e.EVENT_ID as EVENT_ID, p.EVENT_DATE as EPISODE_START_DATE,
		  'CONTRA' as category,
			row_number() over (partition by e.person_id, e.event_id order by p.EVENT_DATE desc) as rn
		from cteOutcomeEvents e
		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
		WHERE p.Category in  ('CONTRA')
			and p.EVENT_DATE between
				case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
				else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
				and dateadd(d, -1* lb.MIN_TERM, e.EVENT_DATE)
	) Q
	where rn = 1
),
cteDefaultStartDates(PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
  select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK
  from
  (
  	select c.PERSON_ID, c.EVENT_ID, case when dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE) > dateadd(d,(-1 * (
  		case when PreCount > 0 then g.PreTerm
  		when FullCount > 0 then g.FullTerm
  		else g.NoData end)),EVENT_DATE) then dateadd(d, lb.retry , pod.PRIOR_OUTCOME_DATE)
  		else  dateadd(d,(-1 * (
  		case when PreCount > 0 then g.PreTerm
  		when FullCount > 0 then g.FullTerm
  		else g.NoData end)),EVENT_DATE) end as EPISODE_START_DATE,
  		case when PreCount > 0 then 'PREM'
  		      else 'DEFAULT' end as CATEGORY,
  			 99 as date_rank
  
  	FROM
  	(
  		select e.PERSON_ID, e.EVENT_ID, e.EVENT_DATE, e.CATEGORY,
  			SUM(case when p.Category = 'PREM' then 1 else 0 end) as PreCount,
  			SUM(case when p.Category = 'POSTT' then 1 else 0 end) as PostCount,
  			SUM(case when p.Category = 'FT' then 1 else 0 end) as FullCount
  		from cteOutcomeEvents e
  		JOIN @resultsDatabaseSchema.term_durations lb on e.Category = lb.CATEGORY
  		JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = e.PERSON_ID and pod.EVENT_ID = e.EVENT_ID
  		JOIN #pregnancy_events p on e.PERSON_ID = p.PERSON_ID
  		where p.EVENT_DATE between
  			case when pod.PRIOR_OUTCOME_DATE > dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) then pod.PRIOR_OUTCOME_DATE
  			else dateadd(d, -1* lb.MAX_TERM, e.EVENT_DATE) end
  			and dateadd(d,30,e.EVENT_DATE)
  		GROUP BY e.PERSON_ID, e.EVENT_ID, e.EVENT_DATE, e.CATEGORY
  	) C JOIN @resultsDatabaseSchema.gest_est g on c.CATEGORY = g.CATEGORY
  	full outer JOIN ctePriorOutcomeDates pod on pod.PERSON_ID = c.PERSON_ID and pod.EVENT_ID = c.EVENT_ID
  	full outer JOIN @resultsDatabaseSchema.term_durations lb on pod.prior_category = lb.CATEGORY
  ) Q
),
cteAllStarts(PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteLMPStartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteDefaultStartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteGestStartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteNuchalUltrasoundStartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteAFPStartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteOvulStartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteOvul2StartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from cteAMENStartDates
	UNION
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK from ctePOptumStartDates
	-- UNION CTEs together to collect all date selections based on individual rules for each category
),
cteBestStarts (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select PERSON_ID, EVENT_ID, cast(EPISODE_START_DATE as date), CATEGORY, DATE_RANK
	FROM
	(
		SELECT PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK,
		ROW_NUMBER() OVER (PARTITION BY PERSON_ID, EVENT_ID ORDER BY DATE_RANK) as RN
		FROM cteAllStarts s
	) Q
	where Q.RN = 1
),
cteNewStarts (PERSON_ID, EVENT_ID, EPISODE_START_DATE, CATEGORY, DATE_RANK) as
(
	select bs.PERSON_ID,
    bs.EVENT_ID,
		case
		  when pc.episode_start_date is null and cc.episode_start_date is null then bs.episode_start_date
			when cc.episode_start_date is null and pc.EPISODE_START_DATE < bs.EPISODE_START_DATE then pc.EPISODE_START_DATE
		  when pc.episode_start_date is null and cc.EPISODE_START_DATE > bs.EPISODE_START_DATE then cc.EPISODE_START_DATE
			when pc.episode_start_date is not null and cc.episode_start_date is not null
			  and pc.EPISODE_START_DATE < bs.EPISODE_START_DATE and cc.EPISODE_START_DATE<  pc.EPISODE_START_DATE then pc.EPISODE_START_DATE
  		when pc.episode_start_date is not null and cc.episode_start_date is not null
  		  and pc.EPISODE_START_DATE < bs.EPISODE_START_DATE and cc.EPISODE_START_DATE>  pc.EPISODE_START_DATE then cc.EPISODE_START_DATE
			when pc.episode_start_date is not null and cc.episode_start_date is not null
			  and cc.EPISODE_START_DATE > bs.EPISODE_START_DATE and pc.episode_start_date>bs.EPISODE_START_DATE then cc.EPISODE_START_DATE
			else bs.episode_start_date
		end as episode_start_date,
    bs.CATEGORY+':REFINED' as CATEGORY,
    bs.DATE_RANK-1
	FROM cteBestStarts bs
	left outer join ctePCONFStartDates pc on bs.person_id = pc.person_id and bs.event_id = pc.event_id
	left outer join cteCONTRAStartDates cc on bs.person_id = cc.person_id and bs.event_id = cc.event_id
	where bs.date_rank > 7
)
select o.PERSON_ID, s.EPISODE_START_DATE, o.EVENT_DATE as EPISODE_END_DATE,s.CATEGORY as START_CATEGORY,
o.CATEGORY as OUTCOME_CATEGORY, s.DATE_RANK as rank
INTO #PregnancyEpisodes
from cteOutcomeEvents o
join (
  select * from cteAllStarts
  union
  select * from cteNewStarts
) s on o.person_id = s.person_id and o.EVENT_ID = s.EVENT_ID
;

/* save all possible starts here */

select person_id, episode_start_date, episode_end_date, start_category, outcome_category, rank
INTO #PregnancyEpisodesAllStarts
from #PregnancyEpisodes
;

/* choose first start in hierarchy per episode, female 12-55 only, episode within enrollment period */

select pe.PERSON_ID, pe.EPISODE_START_DATE, pe.EPISODE_END_DATE, pe.START_CATEGORY, pe.OUTCOME_CATEGORY,
row_number() over (partition by pe.person_id order by pe.Episode_start_date) as rn
into #PregnancyEpisodesObs
from
(
	select *
	FROM
	(
		SELECT *, ROW_NUMBER() OVER (PARTITION BY PERSON_ID, EPISODE_END_DATE ORDER BY RANK) as RN
		FROM #PregnancyEpisodes s
	) Q
	where Q.RN = 1
) pe
JOIN @cdmDatabaseSchema.OBSERVATION_PERIOD op on op.PERSON_ID = pe.PERSON_ID
	and episode_end_date between op.OBSERVATION_PERIOD_START_DATE and op.OBSERVATION_PERIOD_END_DATE
	and pe.EPISODE_START_DATE>= op.observation_period_start_date
	join @cdmDatabaseSchema.person p
	on pe.person_id=p.person_id
	where p.gender_concept_id = 8532 and year(pe.episode_start_date)-p.year_of_birth>=12
	and year(pe.episode_start_date)-p.year_of_birth<=55
;

/* verify that there are at least 2 pregnancy events for an outcome */

select f.PERSON_ID,f.EPISODE_START_DATE,f.EPISODE_END_DATE,f.START_CATEGORY,f.OUTCOME_CATEGORY,f.RN
into #PregnancyEpisodesTwoRec
from #PregnancyEpisodesObs f
join
(
	select *
	from
	(
		select person_id, rn, count(event_id) as tot_events
		from
		(
			select a.person_id, a.rn, b.event_id
			from #PregnancyEpisodesObs a
			join  #pregnancy_events b on a.person_id=b.person_id
			JOIN @resultsDatabaseSchema.term_durations d on a.OUTCOME_CATEGORY=d.category
			where b.EVENT_DATE between dateadd(d,((-1 * d.MAX_TERM) + 1), a.EPISODE_END_DATE) and a.EPISODE_END_DATE
		) c
		group by person_id, rn
	) d
	where tot_events>=2
) e on e.person_id=f.person_id and e.rn=f.rn
;

/* create the phenotype */

insert into @resultsDatabaseSchema.pregnancy_episodes
select
  person_id as person_id,
  episode_start_date as episode_start_date,
  episode_end_date as episode_end_date,
  start_category as start_method, outcome_category as original_outcome,
	rn as episode,
	case when outcome_category in ('AB','SA') then 'SA/AB'
    when outcome_category in ('DELIV','LB') then 'LB/DELIV'
	  when outcome_category='SB' and datediff(dd,episode_start_date, episode_end_date)+1<140 then 'SA/AB'
	  when (outcome_category='SA' or outcome_category='AB') and datediff(dd,episode_start_date, episode_end_date)+1<140 then 'SA/AB'
	else outcome_category end as outcome,
	datediff(dd,episode_start_date, episode_end_date)+1 as episode_length
from #PregnancyEpisodesTwoRec
;

TRUNCATE TABLE #ValidOutcomes;
DROP TABLE #ValidOutcomes;

TRUNCATE TABLE #pregnancy_events;
DROP TABLE #pregnancy_events;

TRUNCATE TABLE #PregnancyEpisodes;
DROP TABLE #PregnancyEpisodes;

TRUNCATE TABLE #PregnancyEpisodesAllStarts;
DROP TABLE #PregnancyEpisodesAllStarts;

TRUNCATE TABLE #PregnancyEpisodesObs;
DROP TABLE #PregnancyEpisodesObs;

TRUNCATE TABLE #PregnancyEpisodesTwoRec;
DROP TABLE #PregnancyEpisodesTwoRec;
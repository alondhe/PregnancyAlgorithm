IF OBJECT_ID('@resultsDatabaseSchema.FirstOutcomeEvent', 'U') IS NOT NULL
  DROP TABLE @resultsDatabaseSchema.FirstOutcomeEvent;

IF OBJECT_ID('@resultsDatabaseSchema.pregnancy_episodes', 'U') IS NOT NULL
  DROP TABLE @resultsDatabaseSchema.pregnancy_episodes;

--HINT DISTRIBUTE_ON_KEY(PERSON_ID)
CREATE TABLE @resultsDatabaseSchema.pregnancy_episodes
(
	PERSON_ID bigint not null,
	EPISODE_START_DATE datetime not null,
	EPISODE_END_DATE datetime not null,
	START_METHOD nvarchar(255) not null,
	ORIGINAL_OUTCOME nvarchar(255) not null,
	EPISODE int not null,
	OUTCOME nvarchar(255) not null,
	EPISODE_LENGTH int
)
;

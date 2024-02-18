-- deal_facts


WITH date_max AS (
    -- selecing most recent date for the pipeline stages
    -- this is to capture deals that have moved into stages multiple times
    SELECT
        DEAL_ID,
        MAX(STAGE_CREATED_AT) OVER (PARTITION BY DEAL_ID ORDER BY STAGE_CREATED_AT DESC) as STAGE_CREATED_AT,
        STAGE_DISPLAY_ORDER
    FROM 
        `country_roads.deal_pipeline_stages` 
    ) ,

process AS ( 
    -- creating table with relevant stage timestamps
    SELECT 
    d.DEAL_ID,
    d.DEAL_SOURCE_TYPE,
    d.CREATED_AT,
    d.CLOSE_DATE as CLOSED_AT,
    d.STAGE_NAME as CURRENT_STAGE,
    d.AMOUNT_IN_HOME_CURRENCY,
    CASE WHEN date_max.STAGE_DISPLAY_ORDER = 5 THEN date_max.STAGE_CREATED_AT ELSE null end AS DISCOVERY_TS, --stage 1
    CASE WHEN date_max.STAGE_DISPLAY_ORDER = 6 THEN date_max.STAGE_CREATED_AT ELSE null end AS QUALIFIED_TS, --stage 2
    CASE WHEN date_max.STAGE_DISPLAY_ORDER = 7 THEN date_max.STAGE_CREATED_AT ELSE null end AS PROOF_OF_VALUE_TS, --stage 3
    CASE WHEN date_max.STAGE_DISPLAY_ORDER = 8 THEN date_max.STAGE_CREATED_AT ELSE null end AS PROPOSAL_TS, --stage 4
    CASE WHEN date_max.STAGE_DISPLAY_ORDER = 9 THEN date_max.STAGE_CREATED_AT ELSE null end AS PROCUREMENT_TS, --stage 5
    CASE WHEN date_max.STAGE_DISPLAY_ORDER = 10 THEN date_max.STAGE_CREATED_AT ELSE null end AS CLOSED_WON_TS, --stage 6 Y
    CASE WHEN date_max.STAGE_DISPLAY_ORDER = 2 THEN date_max.STAGE_CREATED_AT ELSE null end AS CLOSED_LOST_TS, --stage 6 N
    FROM `country_roads.deals` d
    LEFT JOIN date_max ON d.DEAL_ID = date_max.DEAL_ID
    )

SELECT 
-- formatting deal_facts with struct of pipeline stage timestamps
    DEAL_ID,
    DEAL_SOURCE_TYPE, 
    CURRENT_STAGE,
    CREATED_AT,
    CLOSED_AT,
    AMOUNT_IN_HOME_CURRENCY,
    STRUCT(
            DISCOVERY_TS,
            QUALIFIED_TS,
            PROOF_OF_VALUE_TS,
            PROPOSAL_TS,
            PROCUREMENT_TS,
            CLOSED_WON_TS,
            CLOSED_LOST_TS
            ) pipeline 
FROM
    (
        -- cleaning up nulls introduced in process table, deduplicating using
        SELECT 
            DEAL_ID,
            DEAL_SOURCE_TYPE,
            CURRENT_STAGE,
            CREATED_AT,
            CLOSED_AT,
            AMOUNT_IN_HOME_CURRENCY,
            ARRAY_AGG(distinct DISCOVERY_TS IGNORE NULLS) DISCOVERY_TS,
            ARRAY_AGG(distinct QUALIFIED_TS IGNORE NULLS) QUALIFIED_TS,
            ARRAY_AGG(distinct PROOF_OF_VALUE_TS IGNORE NULLS) PROOF_OF_VALUE_TS,
            ARRAY_AGG(distinct PROPOSAL_TS IGNORE NULLS) PROPOSAL_TS,
            ARRAY_AGG(distinct PROCUREMENT_TS IGNORE NULLS) PROCUREMENT_TS,
            ARRAY_AGG(distinct CLOSED_WON_TS IGNORE NULLS) CLOSED_WON_TS,
            ARRAY_AGG(distinct CLOSED_LOST_TS IGNORE NULLS) CLOSED_LOST_TS
        FROM process    
        GROUP BY 1,2,3,4,5,6
    )

LEFT JOIN UNNEST(DISCOVERY_TS) DISCOVERY_TS with OFFSET
LEFT JOIN UNNEST(QUALIFIED_TS) QUALIFIED_TS with OFFSET USING(OFFSET)
LEFT JOIN UNNEST(PROOF_OF_VALUE_TS) PROOF_OF_VALUE_TS with OFFSET USING(OFFSET)
LEFT JOIN UNNEST(PROPOSAL_TS) PROPOSAL_TS with OFFSET USING(OFFSET)
LEFT JOIN UNNEST(PROCUREMENT_TS) PROCUREMENT_TS with OFFSET USING(OFFSET)
LEFT JOIN UNNEST(CLOSED_WON_TS) CLOSED_WON_TS with OFFSET USING(OFFSET)
LEFT JOIN UNNEST(CLOSED_LOST_TS) CLOSED_LOST_TS with OFFSET USING(OFFSET)
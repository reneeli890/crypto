-- Query to find daily supply of a given coin, using WBTC for example.
-- Although the below code just represents one model with multiple CTEs, the following is how I would architect the query and separate it into different models in order to optimize for compute.

-- Model 1: staging_contract
-- This filters the source table to just the contract and functions we’re interested in. I chose to leverage a variable for contract_address, because that is what I would do if this were a real project and I wanted to scale this model to be able to be used for any contract_address, without changing the underlying code. 
-- Additional notes: Since this is merely filtering the source table, aside from exploding out the json fields, I would actually materialize this as a view since no complex transformations are taking place.

-- Model 2: total_by_func
-- In this model, the CTE supply_by_date_func pulls from staging_contract and aggregates transaction value and transaction amount by day and func. I noticed that when func = mint, then amount is present, but when func = burn, then value is present. This explains the sum/case when function I used for calculating total satoshi. Additionally, it converts the sum from satoshis to btc. I realized it was in satoshis because one, the sums are way too much and two, I confirmed this by going to etherscan and checking for a given transaction hash; indeed the WBTC value was 1/1e8 of the sum. 
-- Finally, I use a sum/case when function to make total_burned and total_minted their own columns.
-- 
-- Model 3: total_daily_supply
-- In this model, the daily_deltas CTE pulls from total_by_func and also calculates a daily delta as total_minted - total_burned. This tells us the difference in supply per day.
-- Then the rolling_delta CTE lags the daily delta by 1 day so then the next day’s total supply is calculated by the previous day’s total supply plus the daily delta. 
-- Validation: If I actually ran this whole project through, the most recent WBTC ending supply should equal the total supply we see in etherscan.


-- Additional notes:
-- Date spine 
-- I realized later that not every day is accounted for in the source table, so I would have made a date spine to populate each day since inception, then add that into the total_daily_supply model by left joining the rolling_delta CTE to recalculate the lagging daily deltas. I would also need to persist the last existing date’s total_supply through all the consecutive dates that were absent, because currently the total_supply is not being carried through non-consecutive days.



-- Code:

WITH SELECT_COIN AS (
    
  SELECT
  EVT_BLOCK_TIME,
  NAMESPACE,
  NAME,
  FUNC,
  SIG,
  CONTRACT_ADDRESS,
  EVT_BLOCK_NUMBER,
  EVT_BLOCK_HASH,
  EVT_TX_HASH,
  EVT_TX_INDEX,
  EVT_INDEX,
  DATA,
  JSON_EXTRACT_PATH_TEXT(A, 'from')
      AS TXN_FROM,
  JSON_EXTRACT_PATH_TEXT(A, 'to')
      AS TXN_TO,
  JSON_EXTRACT_PATH_TEXT(A, 'amount')
      AS TXN_AMOUNT_SATHOSHI, -- ASSUMING THIS IS IN SATOSHI, BASED ON COMPARING A TXN HASH ON ETHERSCAN
    JSON_EXTRACT_PATH_TEXT(A, 'value')
    AS TXN_VALUE_SATOSHI
  FROM ANALYTICS_ENGINEER_SHARE_INBOUND.SHARED.ETHEREUM_MAINNET_CONTRACT_EVENTS
  WHERE CONTRACT_ADDRESS = '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
  AND UPPER(FUNC) IN ('BURN','MINT')

),

SUPPLY_BY_DATE_FUNC AS (

  SELECT
  DATE(EVT_BLOCK_TIME) AS DATE,
  FUNC,
  SUM(CASE WHEN UPPER(FUNC) = 'MINT' THEN TXN_AMOUNT_SATHOSHI
      WHEN UPPER(FUNC) = 'BURN' THEN TXN_VALUE_SATOSHI
      END) AS TOTAL_SATOSHI -- IT SEEMS THAT IF FUNC = MINT, THEN TXN_AMOUNT IS PRESENT, IF FUNC = BURN, THEN TXN_VALUE IS USED

  FROM SELECT_COIN
  GROUP BY DATE(EVT_BLOCK_TIME), FUNC
  ORDER BY DATE
),

SUPPLY_BY_DATE_FUNC_BTC AS (
  
  SELECT
    *,
    TOTAL_SATOSHI/100000000 AS TOTAL_BTC
  
  FROM SUPPLY_BY_DATE_FUNC
),

TOTAL_BY_FUNC AS (
  
  SELECT 
    DATE,
    COALESCE(SUM(CASE WHEN UPPER(FUNC) LIKE 'BURN' THEN TOTAL_BTC END),0) AS TOTAL_BURNED, -- WHERE TXN FROM = NULL ADDRESS IS MINT, WHERE TXN TO = NULL ADDRESS IS BURN
    COALESCE(SUM(CASE WHEN UPPER(FUNC) LIKE 'MINT' THEN TOTAL_BTC END),0) AS TOTAL_MINTED

  FROM SUPPLY_BY_DATE_FUNC_BTC
  GROUP BY DATE
  ORDER BY DATE
),

DAILY_DELTAS AS ( 
  SELECT
    *,
    COALESCE((TOTAL_MINTED- TOTAL_BURNED),0) AS DAILY_DELTA
  
  FROM TOTAL_BY_FUNC
  ORDER BY DATE
),

ROLLING_DELTA AS (
  SELECT
    *,
    COALESCE(LAG(DAILY_DELTA) OVER (PARTITION BY DATE ORDER BY DATE),0) AS PREVIOUS_DATE_DELTA

  FROM DAILY_DELTAS
  ORDER BY DATE
  
)

SELECT
    *,
    (PREVIOUS_DATE_DELTA + DAILY_DELTA) AS TOTAL_SUPPLY
    
    FROM ROLLING_DELTA
    ORDER BY DATE
;


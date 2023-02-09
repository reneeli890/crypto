-- copying over my work in Zettablock. https://app.zettablock.com/my-workspace/queries/qu5-ace318ae-0f2b-4524-be7e-44059efb85da
-- Here I'm just exploring transaction logs and price data using eth and polygon mainnets. You can build upon this in many ways but here I am looking at the price of MATIC at the time of a given transaction.


-- filter transaction table
with transactions as 
  (
    SELECT
    "hash",
    "transaction_index",
    "from_address", 
    "to_address", 
    "block_number",
    "value"/power(10,18) as total_sell,
    "gas_price"/power(10,18) as total_fee,
    "data_creation_date"
  
    FROM
    polygon_mainnet.transactions 
    WHERE
      "hash" = '0xf50a90c757889e0af298497bc2f9e0ec40ce1bb503ed19cc0e399cd002204404' -- just picking one hash to work off of as an example
  )

  -- each transaction has 1 nft transfer (but can have several erc20 token transfers and therefore can have several token_ids)
  , nft_data as 
  (
    select
      erc721.transaction_hash,
      erc721.block_time,
      erc721.data_creation_date, 
      erc721.token_id,
      erc721.contract_address,
      erc721.from_address as seller,
      erc721.to_address as buyer,
      nfts.name,
      nfts.symbol
      
      from polygon_mainnet.erc721_evt_transfer erc721 
      left join polygon_mainnet.nft_tokens nfts on erc721.contract_address = nfts.contract_address
      WHERE
        "transaction_hash" = '0xf50a90c757889e0af298497bc2f9e0ec40ce1bb503ed19cc0e399cd002204404' -- just picking one hash to work off of as an example
  ),

  -- each transaction can have several erc20 token transfers
 log_enhanced as 
  (
  SELECT
  logs."transaction_hash",
  logs."transaction_index",
  logs."block_number",
  logs."block_hash",
  logs."log_index",
  logs."data",
  logs."topics",
  logs."contract_address",
  logs."block_time",
  logs."process_time",
  logs."data_creation_date",
  dec_logs."event"
FROM
  polygon_mainnet.logs logs
  left join polygon_mainnet.decoded_logs dec_logs
  on logs.transaction_hash = dec_logs.transaction_hash
  and logs.log_index = dec_logs.log_index
  and logs.block_hash = dec_logs.block_hash
where logs."transaction_hash" = '0xf50a90c757889e0af298497bc2f9e0ec40ce1bb503ed19cc0e399cd002204404'
  
),

  -- augment the log enhanced CTE with contract data
log_contract_data as 
  (
  select 
  log_enhanced.*,
  contract_mappings."contract_name", 
  contract_mappings."contract_token_type"
  from log_enhanced
  left join polygon_mainnet.contract_mappings  
    on log_enhanced."contract_address" = contract_mappings."contract_address"
  where log_enhanced."transaction_hash" = '0xf50a90c757889e0af298497bc2f9e0ec40ce1bb503ed19cc0e399cd002204404'
  )
-- map each transaction to matic's usd price at that time of transaction
  
SELECT
  log_contract_data.*,
  prices."name",
  prices."symbol",
  prices."price",
  transactions.total_fee
FROM log_contract_data
  left join prices.usd as prices
  on date_trunc('minute', log_contract_data.block_time) = prices.minute -- we want the price of the token at the time the block was processed
  left join transactions
  on log_contract_data."transaction_hash" = transactions.hash
  where lower("name") like '%matic%'
  limit 1
  
;

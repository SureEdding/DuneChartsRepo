SELECT
  block_time,
  BYTEA2NUMERIC(SUBSTRING(data FROM 1 FOR 32)) / 1e18 AS BOND_DAI_INPUT,
  BYTEA2NUMERIC(SUBSTRING(data FROM 33 FOR 32)) / 1e18 AS BOND_CNV_OUTPUT,
  date_trunc('day', block_time), SUM(BYTEA2NUMERIC(SUBSTRING(data FROM 1 FOR 32)) / 1e18) OVER (ORDER BY date_trunc('day', block_time)) as CUMULATIVE_DAI_BONDED,
  date_trunc('day', block_time), SUM(BYTEA2NUMERIC(SUBSTRING(data FROM 33 FOR 32)) / 1e18) OVER (ORDER BY date_trunc('day', block_time)) as CUMULATIVE_CNV_BONDED,
  date_trunc('day', block_time), SUM(BYTEA2NUMERIC(SUBSTRING(data FROM 1 FOR 32)) / 1e18) OVER (ORDER BY date_trunc('day', block_time)) / SUM(BYTEA2NUMERIC(SUBSTRING(data FROM 33 FOR 32)) / 1e18)  OVER (ORDER BY date_trunc('day', block_time)) as AVERAGE_BOND_PRICE,
  (BYTEA2NUMERIC(SUBSTRING(data FROM 1 FOR 32)) / 1e18) / (BYTEA2NUMERIC(SUBSTRING(data FROM 33 FOR 32)) / 1e18) as ORDER_BOND_PRICE,
  avg((BYTEA2NUMERIC(SUBSTRING(data FROM 1 FOR 32)) / 1e18) / (BYTEA2NUMERIC(SUBSTRING(data FROM 33 FOR 32)) / 1e18)) over (order by block_time rows between 119 preceding and current row) as BOND_PRICE_SMA_120,
  avg(BYTEA2NUMERIC(SUBSTRING(data FROM 1 FOR 32)) / 1e18) over (order by block_time rows between 119 preceding and current row) as DAI_BOND_VOLUME_SMA_120,
  avg(BYTEA2NUMERIC(SUBSTRING(data FROM 33 FOR 32)) / 1e18) over (order by block_time rows between 119 preceding and current row) as CNV_BOND_VOLUME_SMA_120

FROM
  ethereum."logs"
WHERE
  contract_address = '\xe42bce7bd1a94f99a099ee9242aa0f3b2f5b1d50'
  AND topic1 = '\x8e5101242b74cdfce3a74e64844cf2cc76186195bf6d8cfe04c7f519f64dfbb3'
  ORDER BY block_time desc
  ;
  
SELECT
    BOND_RECORDS.bonder_address as bonder_address,
    ENS.ens_names as ens_names,
    SUM(BOND_RECORDS.BOND_DAI_INPUT) as bond_amount_in_usd,
    COUNT(BOND_RECORDS.BOND_DAI_INPUT) as bond_count
FROM (
SELECT
  block_time,
  CONCAT('\x', SUBSTRING(encode(topic2, 'hex') FROM 25 )) as BONDER_ADDRESS,
  BYTEA2NUMERIC(SUBSTRING(data FROM 1 FOR 32)) / 1e18 AS BOND_DAI_INPUT,
  BYTEA2NUMERIC(SUBSTRING(data FROM 33 FOR 32)) / 1e18 AS BOND_CNV_OUTPUT
FROM
  ethereum."logs"
WHERE
  contract_address = '\xe42bce7bd1a94f99a099ee9242aa0f3b2f5b1d50'
  AND topic1 = '\x8e5101242b74cdfce3a74e64844cf2cc76186195bf6d8cfe04c7f519f64dfbb3'
  ORDER BY block_time desc
) as BOND_RECORDS 
LEFT JOIN (
select
  concat('\x', encode(owner, 'hex')) as owner_address,
  array_agg(concat(name,'.eth')) as ens_names
From
  ethereumnameservice.view_registrations
where
  to_timestamp(expires) > now()
group by
  owner_address
) as ENS 
on ENS.owner_address = BOND_RECORDS.BONDER_ADDRESS
GROUP BY BOND_RECORDS.BONDER_ADDRESS, ens_names
ORDER BY bond_amount_in_usd desc;

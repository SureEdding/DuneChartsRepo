WITH swap AS
(
	SELECT  sw."evt_block_time"               AS minute
	       ,("amount0In" + "amount0Out")/1e9  AS a0_amt
	       ,("amount1In" + "amount1Out")/1e18 AS a1_amt
	FROM sushi."Pair_evt_Swap" sw
	WHERE contract_address = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c' -- liq pair OHM-DAI 
), price AS
(
	SELECT  swap."minute"
	       ,(a1_amt/a0_amt) AS price
	FROM swap
	ORDER BY 1 desc
), bond_price AS
(
	SELECT  evt_block_time    AS minute
	       ,deposit/1e18      AS deposit
	       ,payout/1e9        AS payout
	       ,expires
	       ,"priceInUSD"/1e18 AS bond_price
	       ,contract_address
	       ,evt_tx_hash
	       ,evt_index
	       ,evt_block_number
	FROM olympus."OlympusBondDepository_evt_BondCreated"
	WHERE ("priceInUSD"/1e18)*(payout/1e9) > 20
	AND evt_tx_hash != '\x15ad11aa3d9ca8aebe840ebc0defd03bc374356ff6c112d83464160f6bd86b82' 
), discount AS
(
	SELECT  minute
	       ,(
	SELECT  price
	FROM price
	WHERE price.minute <= bond_price.minute
	ORDER BY bond_price.minute
	LIMIT 1) AS price, bond_price, 1/bond_price AS yield, ((
	SELECT  price
	FROM price
	WHERE price.minute <= bond_price.minute
	ORDER BY bond_price.minute
	LIMIT 1)-bond_price)/bond_price AS discount, contract_address, evt_tx_hash
	FROM bond_price
	ORDER BY 1 desc
)
SELECT  minute
       ,price
       ,bond_price
       ,yield
       ,yield * 100000                                                                            AS expanded_yield
       ,discount
       ,AVG(discount) OVER(ORDER BY minute ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)             AS "discount_90_ma"
       ,contract_address
       ,CASE WHEN contract_address = '\x575409F8d77c12B05feD8B455815f0e54797381c' THEN 'DAI'
             WHEN contract_address = '\x956c43998316b6a2F21f89a1539f73fB5B78c151' THEN 'OHM-DAI'
             WHEN contract_address = '\x8510c8c2B6891E04864fa196693D44E6B6ec2514' THEN 'FRAX'
             WHEN contract_address = '\xc20CffF07076858a7e642E396180EC390E5A02f7' THEN 'OHM-FRAX'
             WHEN contract_address = '\xe6295201cd1ff13ced5f063a5421c39a1d236f1c' THEN 'wETH'
             WHEN contract_address = '\x10C0f93f64e3C8D0a1b0f4B87d6155fd9e89D08D' THEN 'LUSD' END AS asset
       ,evt_tx_hash
FROM discount
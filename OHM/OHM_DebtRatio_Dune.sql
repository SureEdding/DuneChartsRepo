WITH Supply AS
(
	SELECT  "date"
	       ,total_supply
	FROM dune_user_generated.ohm_circ_supply
	ORDER BY "date"
), RiskFreeValue AS
(
	SELECT  "date"
	       ,treasury_rfv
	       ,treasury_lusd + (treasury_slp_ohmlusd / coalesce(NULLIF(slp_supply_ohmlusd,0),1) )*(2*sqrt(lp_lusd* l_lp_ohm)) AS lusd_rfv
	       ,treasury_frax + (treasury_univ2 / coalesce(NULLIF(univ2_supply,0),1) )*(2*sqrt(lp_frax * f_lp_ohm))            AS frax_rfv
	       ,(treasury_dai + (slp_treasury/slp_supply)*(2*sqrt(lp_dai * lp_ohm)))                                           AS dai_rfv
	FROM dune_user_generated.treasury_rfv
	ORDER BY "date"
)
SELECT  supply."date"                                                   AS "finalDate"
       ,supply.total_supply
       ,rfv.dai_rfv
       ,(rfv.dai_rfv + rfv.lusd_rfv + rfv.frax_rfv)/supply.total_supply AS "total_debt_ratio"
       ,(rfv.dai_rfv/supply.total_supply)                               AS dai_debt_ratio
       ,(rfv.lusd_rfv/supply.total_supply)                              AS lusd_debt_ratio
       ,(rfv.frax_rfv/supply.total_supply)                              AS frax_debt_ratio
FROM
(
	SELECT  "date"
	       ,total_supply
	FROM Supply
) supply
JOIN
(
	SELECT  "date"
	       ,treasury_rfv
	       ,lusd_rfv
	       ,frax_rfv
	       ,dai_rfv
	FROM RiskFreeValue
) rfv
ON supply."date" = rfv."date"
ORDER BY "finalDate"
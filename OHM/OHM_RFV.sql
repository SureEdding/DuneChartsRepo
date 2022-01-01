SELECT  "date"
       ,treasury_rfv
       ,treasury_lusd + (treasury_slp_ohmlusd / coalesce(NULLIF(slp_supply_ohmlusd,0),1) )*(2*sqrt(lp_lusd* l_lp_ohm)) AS lusd_rfv
       ,treasury_frax + (treasury_univ2 / coalesce(NULLIF(univ2_supply,0),1) )*(2*sqrt(lp_frax * f_lp_ohm))            AS frax_rfv
       ,(treasury_dai + (slp_treasury/slp_supply)*(2*sqrt(lp_dai * lp_ohm)))                                           AS dai_rfv
FROM dune_user_generated.treasury_rfv
with 

Supply AS (
    select "date",total_supply
    from dune_user_generated.ohm_circ_supply
    order by "date"
),

RiskFreeValue AS (
    select 
        "date", 
        treasury_rfv,
        treasury_lusd + (treasury_slp_ohmlusd / coalesce(NULLIF(slp_supply_ohmlusd,0),1) )*(2*sqrt(lp_lusd* l_lp_ohm)) as lusd_rfv,
        treasury_frax + (treasury_univ2 / coalesce(NULLIF(univ2_supply,0),1) )*(2*sqrt(lp_frax * f_lp_ohm)) as frax_rfv,
        (treasury_dai + (slp_treasury/slp_supply)*(2*sqrt(lp_dai * lp_ohm))) as dai_rfv
    from dune_user_generated.treasury_rfv
    order by "date"
)

select 
    supply."date" as "finalDate",
    supply.total_supply,
    rfv.dai_rfv,
    (rfv.dai_rfv + rfv.lusd_rfv + rfv.frax_rfv)/supply.total_supply as "total_debt_ratio",
    (rfv.dai_rfv/supply.total_supply) as dai_debt_ratio,
    (rfv.lusd_rfv/supply.total_supply) as lusd_debt_ratio,
    (rfv.frax_rfv/supply.total_supply) as frax_debt_ratio
from (
    select "date", total_supply from Supply
) supply 
join (
    select "date", treasury_rfv, lusd_rfv, frax_rfv, dai_rfv
    from RiskFreeValue
) rfv on supply."date" = rfv."date" 
order by "finalDate"


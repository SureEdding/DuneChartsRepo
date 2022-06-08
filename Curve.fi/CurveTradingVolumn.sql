select day, sum(TriPool) as TriPool, sum(mim) as mim, sum(ren) as ren, sum(susdv2) as susdv2, sum(busd) as busd, sum(y) as y, sum(usdt) as usdt, sum(pax) as pax, sum(compound) as compound from (

select date_trunc('day', evt_block_time) AS day, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
end)) as TriPool, 0 as ren, 0 as susdv2, 0 as busd, 0 as y, 0 as usdt, 0 as pax, 0 as compound, 0 as mim from curvefi."threepool_swap_evt_TokenExchange" as cst
group by 1

union


(select date_trunc('day', evt_block_time) AS day, 0 as ren, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
when sold_id = 3 then 1e18
end)) susdv2, 0 as busd, 0 as y, 0 as usdt, 0 as pax, 0 as compound, 0 as mim, 0 as TriPool from curvefi."susd_swap_evt_TokenExchangeUnderlying" as cst
group by 1

union

select date_trunc('day', evt_block_time) AS day, 0 as ren, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
when sold_id = 3 then 1e18
end)) susdv2, 0 as busd, 0 as y, 0 as usdt, 0 as pax, 0 as compound, 0 as mim, 0 as TriPool from curvefi."susd_swap_evt_TokenExchange" as cst
group by 1
)


union


select date_trunc('day', evt_block_time) AS day, 0 as ren, 0 as susdv2, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
when sold_id = 3 then 1e18
end)) busd, 0 as y, 0 as usdt, 0 as pax, 0 as compound, 0 as mim, 0 as TriPool from curvefi."busd_swap_evt_TokenExchangeUnderlying" as cst
group by 1


union


select date_trunc('day', evt_block_time) AS day, 0 as ren, 0 as susdv2, 0 as busd, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
when sold_id = 3 then 1e18
end)) y, 0 as usdt, 0 as pax, 0 as compound, 0 as mim, 0 as TriPool from curvefi."y_swap_evt_TokenExchangeUnderlying" as cst
group by 1

union


select date_trunc('day', evt_block_time) AS day, 0 as ren, 0 as susdv2, 0 as busd, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
when sold_id = 3 then 1e18
end)) mim, 0 as usdt, 0 as pax, 0 as compound, 0 as TriPool, 0 as usdt from curvefi."mim_evt_TokenExchange" as cst
group by 1


union


select date_trunc('day', evt_block_time) AS day, 0 as ren, 0 as susdv2, 0 as busd, 0 as y, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
end)) as usdt, 0 as pax, 0 as compound, 0 as mim, 0 as TriPool  from curvefi."usdt_swap_evt_TokenExchangeUnderlying" as cst
group by 1

union


select date_trunc('day', evt_block_time) AS day, 0 as ren, 0 as susdv2, 0 as busd, 0 as y, 0 as usdt, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
when sold_id = 2 then 1e6
when sold_id = 3 then 1e18
end)) as pax, 0 as compound, 0 as mim, 0 as TriPool  from curvefi."usdt_swap_evt_TokenExchangeUnderlying" as cst
group by 1


union


(select date_trunc('day', evt_block_time) AS day, 0 as ren, 0 as susdv2, 0 as busd, 0 as y, 0 as usdt, 0 as pax, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
end)) as compound, 0 as mim, 0 as TriPool from curvefi."compound_v2_evt_TokenExchangeUnderlying" as cst
group by 1
union
select date_trunc('day', evt_block_time) AS day, 0 as ren, 0 as susdv2, 0 as busd, 0 as y, 0 as usdt, 0 as pax, sum(tokens_sold/(case 
when sold_id = 0 then 1e18
when sold_id = 1 then 1e6
end)) as compound, 0 as mim, 0 as TriPool from curvefi."compound_swap_evt_TokenExchangeUnderlying" as cst
group by 1)



) a group by 1
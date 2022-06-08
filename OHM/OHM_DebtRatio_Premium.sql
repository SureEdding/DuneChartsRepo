-- --part 1. are the terms (policies, mainly BCV) in the bond deposit contract fair and managed well? 
-- https://dune.xyz/queries/97819/196696 check target against WETH later 
WITH
    --policy owner is StakingDistributor.sol, they can setBondTerms to adjust the vestingTerms, maxPayout, fee (dao fee), and maxDebt.
    --this large CTE is for max debt, current debt, and vesting terms parameters.
    debt_supply_parameters as (
        WITH    
            max_debt as (
                SELECT date_trunc('day', "call_block_time") "day", "_maxDebt"/1e9 as "maxDebt" FROM olympus."OlympusBondDepository_call_initializeBondTerms" bt 
                WHERE bt."contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
                UNION ALL 
                SELECT date_trunc('day', "call_block_time") "day", "_input"/1e9 as "maxDebt" FROM olympus."OlympusBondDepository_call_setBondTerms"
                WHERE "_parameter" = 3 -- enum DEBT
                AND "contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
            ),
            
            vesting_terms as (
                SELECT date_trunc('day', "call_block_time") "day", "_vestingTerm" FROM olympus."OlympusBondDepository_call_initializeBondTerms" bt 
                WHERE bt."contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
                UNION ALL 
                SELECT date_trunc('day', "call_block_time") "day", "_input" as "_vestingTerm" FROM olympus."OlympusBondDepository_call_setBondTerms"
                WHERE "_parameter" = 1 -- enum VESTING
                AND "contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
            ),
            
            current_debt as (
                --start with first debt
                SELECT 
                    date_trunc('day',"call_block_time") "day",
                    "_initialDebt"/1e9 as "currentDebt",
                    "total_supply"
                FROM olympus."OlympusBondDepository_call_initializeBondTerms" bt 
                LEFT JOIN dune_user_generated.ohm_circ_supply supply ON supply."date" = date_trunc('day',bt."call_block_time")
                WHERE bt."contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
                UNION ALL
                --union with all future debt calculations, taking end of day debt 
                SELECT date_trunc('day', "evt_block_time") "day", "currentDebt", "total_supply"
                FROM (
                    SELECT 
                        bp."evt_block_time", 
                        bp."debtRatio"/1e9 as "debtRatio",
                        (bp."debtRatio"/1e9 * "total_supply") as "currentDebt",
                        "total_supply",
                        bp."evt_tx_hash",
                        row_number() OVER (PARTITION BY date_trunc('day', bp."evt_block_time") ORDER BY bp."evt_block_time" desc) rn
                    FROM olympus."OlympusBondDepository_evt_BondPriceChanged" bp
                    LEFT JOIN dune_user_generated.ohm_circ_supply supply ON supply."date" = date_trunc('day',bp."evt_block_time")
                    WHERE bp."contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
                ) a 
                WHERE rn=1
            ),
            
            values_all_days as (
                SELECT 
                    t."day", 
                    md."maxDebt", 
                    vt."_vestingTerm",
                    cd."currentDebt",
                    cd."total_supply"
                FROM (SELECT generate_series((SELECT MIN("day") FROM max_debt), date_trunc('day', NOW()), '1 day') AS day) t 
                LEFT JOIN max_debt md ON md."day" = t."day"
                LEFT JOIN vesting_terms vt ON vt."day" = t."day"
                LEFT JOIN current_debt cd ON cd."day" = t."day"
            )
            
        SELECT 
            "day",
            first_value("maxDebt") OVER (PARTITION BY "grp_maxdebt" ORDER BY "maxDebt") as "maxDebt",
            first_value("_vestingTerm") OVER (PARTITION BY "grp_vesting" ORDER BY "_vestingTerm") as "_vestingTerm",
            first_value("currentDebt") OVER (PARTITION BY "grp_currentdebt" ORDER BY "currentDebt") as "currentDebt",
            "total_supply"
        FROM (
            SELECT 
                *,
                SUM(CASE WHEN "maxDebt" is not null THEN 1 END) OVER (ORDER BY "day") as "grp_maxdebt",
                SUM(CASE WHEN "_vestingTerm" is not null THEN 1 END) OVER (ORDER BY "day") as "grp_vesting",
                SUM(CASE WHEN "currentDebt" is not null THEN 1 END) OVER (ORDER BY "day") as "grp_currentdebt"
            FROM values_all_days
            ) ac
        ORDER BY "day" ASC
    ),
    
    --select bonding tokens to get total OHM-Dai (0x956c43998316b6a2F21f89a1539f73fB5B78c151) and Dai (0x575409F8d77c12B05feD8B455815f0e54797381c)
    bond_purchases as (
        SELECT 
            date_trunc('day',bc."evt_block_time") "day", 
            SUM("deposit"/1e18) as "Bond Token (Deposited)", 
            SUM("payout"/1e9) as "OHM payout",
            AVG("payout"/"deposit") as "OHM_per_Token",
            -- AVG(bp."priceInUsd") as "priceInUsd",
            AVG(bp."internalPrice") as "bondPrice", 
            AVG(bp."debtRatio") as "debtRatio"
        FROM olympus."OlympusBondDepository_evt_BondCreated" bc 
        LEFT JOIN olympus."OlympusBondDepository_evt_BondPriceChanged" bp ON bp."evt_tx_hash" = bc."evt_tx_hash" 
        WHERE bc."contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
        GROUP BY 1
        ORDER BY 1 ASC
    ),

    --historical BCV policy changes, taking last value of each day
    bcv as (
        SELECT "day", "current_bcv" 
        FROM (
            SELECT 
                date_trunc('day',"evt_block_time") "day", 
                "newBCV" as current_bcv, 
                row_number() OVER (PARTITION BY date_trunc('day',"evt_block_time") ORDER BY "evt_block_time" ASC) rn 
            FROM olympus."OlympusBondDepository_evt_ControlVariableAdjustment"
            WHERE contract_address = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea
        ) a 
        WHERE rn = 1
    ),
    
    days AS 
    (
        SELECT generate_series((SELECT MIN("day") FROM debt_supply_parameters), date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    ),
    
    bcv_filled as (
        SELECT 
            "day", 
            CASE 
                WHEN grp_bcv is null THEN (SELECT "_controlVariable" FROM olympus."OlympusBondDepository_call_initializeBondTerms" bt 
                                            WHERE bt."contract_address" = CONCAT('\x', substring('{{Bond Address}}' from 3))::bytea)
                ELSE first_value("current_bcv") OVER (PARTITION BY "grp_bcv" ORDER BY "current_bcv")
            END "current_bcv"
        FROM (
            SELECT 
                t."day", 
                "current_bcv", 
                SUM(CASE WHEN "current_bcv" is not null THEN 1 END) OVER (ORDER BY t."day") as "grp_bcv"
            FROM "days" t 
            LEFT JOIN bcv ON bcv."day" = t."day"
            ) ac
        ORDER BY "day" ASC
    )

SELECT 
    t."day", 
    bp."Bond Token (Deposited)", 
    bp."debtRatio"/1e9*bcv_filled."current_bcv" as "Premium", 
    bp."debtRatio"/1e9 as "Debt Ratio", 
    bcv_filled."current_bcv" as "BCV",
    bp."bondPrice"/1e4 as "bondPrice", -- 1 + premium
    dsp."maxDebt" as "Max Debt",
    dsp."currentDebt" as "Current Debt",
    dsp."_vestingTerm" as "Vesting Term",
    dsp."total_supply" as "OHM Total Supply"
FROM "days" t
LEFT JOIN bond_purchases bp ON t."day"=bp."day"
LEFT JOIN bcv_filled ON t."day" = bcv_filled."day"
LEFT JOIN debt_supply_parameters dsp ON t."day" = dsp."day"
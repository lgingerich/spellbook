{{ config(
    schema = 'uniswap_v2_ethereum'
    , alias = 'base_liquidity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_liquidity(
        blockchain = 'ethereum'
        , project = 'uniswap'
        , version = '2'
        , Pair_evt_Mint = source('uniswap_v2_ethereum', 'Pair_evt_Mint')
        , Pair_evt_Burn = source('uniswap_v2_ethereum', 'Pair_evt_Burn')
        , Factory_evt_PoolCreated = source('uniswap_v2_ethereum', 'Factory_evt_PoolCreated')
    )
}}
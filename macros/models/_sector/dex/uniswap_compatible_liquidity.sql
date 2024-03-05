{% macro uniswap_compatible_v2_liquidity(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Mint = null
    , Pair_evt_Burn = null
    , Factory_evt_PairCreated = null
    )
%}

WITH add AS (
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'add' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token0 AS token_address
        , p.amount0 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Mint }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}

    UNION ALL
    
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'add' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token1 AS token_address
        , p.amount1 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Mint }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}
)

,remove AS (
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'remove' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token0 AS token_address
        , p.amount0 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Burn }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}

    UNION ALL
    
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'remove' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token1 AS token_address
        , p.amount1 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Burn }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}
)

,union_tbl AS (
    SELECT * FROM add
    UNION ALL
    SELECT * FROM remove
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , transaction_type
    , pair
    , token_address
    , amount
    , tx_hash
    , evt_index
FROM union_tbl
{% endmacro %}

-- -------------------------------------------------------------------------------------

{% macro uniswap_compatible_v3_liquidity(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Mint = null
    , Pair_evt_Burn = null
    , NonfungibleTokenPositionManager_evt_IncreaseLiquidity = null
    , NonfungibleTokenPositionManager_evt_DecreaseLiquidity = null
    , Factory_evt_PairCreated = null
    )
%}

WITH add AS (
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'add' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token0 AS token_address
        , p.amount0 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Mint }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}

    UNION ALL
    
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'add' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token1 AS token_address
        , p.amount1 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Mint }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}

    UNION ALL
    
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'add' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token1 AS token_address
        , p.amount1 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ NonfungibleTokenPositionManager_evt_IncreaseLiquidity }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}

    UNION ALL
    
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'add' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token1 AS token_address
        , p.amount1 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Mint }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}


    
)

,remove AS (
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'remove' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token0 AS token_address
        , p.amount0 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Burn }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}

    UNION ALL
    
    SELECT
        p.evt_block_time AS block_time
        , p.evt_block_number AS block_number
        , 'remove' AS transaction_type
        , t."from" AS provider
        , p.contract_address AS pair
        , f.token1 AS token_address
        , p.amount1 AS amount
        , p.evt_tx_hash AS tx_hash
        , p.evt_index AS evt_index
    FROM {{ Pair_evt_Burn }} p
    LEFT JOIN {{ source('ethereum', 'transactions') }} t ON p.evt_tx_hash = t.hash
    LEFT JOIN {{ Factory_evt_PairCreated }} f ON p.contract_address = f.pair
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}
)

,union_tbl AS (
    SELECT * FROM add
    UNION ALL
    SELECT * FROM remove
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , transaction_type
    , pair
    , token_address
    , amount
    , tx_hash
    , evt_index
FROM union_tbl
{% endmacro %}

-- to use in a SQL File with Cortex Code inline or in the sidebar
-- as instructed in the Quick Demo lecture

-- select this demo schema
use snowflake_sample_data.tpch_sf1;

-- make sure Cortex is available in your region
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Show me the total number of entries in the first table

-- Select top 10 customers from Canada with highest sum of C_ACCTBAL value, in descending order
SELECT
    C.C_CUSTKEY,
    C.C_NAME,
    C.C_ACCTBAL
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
    ON C.C_NATIONKEY = N.N_NATIONKEY
WHERE N.N_NAME = 'CANADA'
ORDER BY C.C_ACCTBAL DESC
LIMIT 10;

-- Show me the total of customers per nation, in ascending order
SELECT 
    N.N_NAME,
    COUNT(C.C_CUSTKEY) AS CUSTOMER_COUNT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
    ON C.C_NATIONKEY = N.N_NATIONKEY
GROUP BY N.N_NAME
ORDER BY CUSTOMER_COUNT ASC;

-- optimize the current query

-- see https://docs.snowflake.com/en/user-guide/sample-data-tpch#functional-query-definition
-- The query lists totals for extended price, discounted extended price, discounted extended price plus tax, average quantity, average extended price, and average discount. These aggregates are grouped by RETURNFLAG and LINESTATUS, and listed in ascending order of RETURNFLAG and LINESTATUS. A count of the number of line items in each group is included:
SELECT
    L_RETURNFLAG,
    L_LINESTATUS,
    SUM(L_EXTENDEDPRICE) AS SUM_EXTENDEDPRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
    AVG(L_QUANTITY) AS AVG_QTY,
    AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
    AVG(L_DISCOUNT) AS AVG_DISC,
    COUNT(*) AS COUNT_ORDER
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS;

/* original demo query
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
 from lineitem
 where l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
 group by l_returnflag, l_linestatus
 order by l_returnflag, l_linestatus;
*/

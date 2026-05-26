-- Columnar Schema for TPC-H Benchmark in Citus
-- Columnar tables do not support primary keys or foreign keys directly.
-- Using native columnar access method to optimize CPU cache and analytical query execution.

-- Set the default shard count. For a 2-socket server, having e.g., 8 shards allows
-- balanced distribution (4 shards per socket) and optimal parallel workers within each socket.
SET citus.shard_count = 8;

-- Create Columnar Tables
CREATE TABLE part (
    p_partkey       INT NOT NULL,
    p_name          VARCHAR(55) NOT NULL,
    p_mfgr          CHAR(25) NOT NULL,
    p_brand         CHAR(10) NOT NULL,
    p_type          VARCHAR(25) NOT NULL,
    p_size          INT NOT NULL,
    p_container     CHAR(10) NOT NULL,
    p_retailprice   DECIMAL(15,2) NOT NULL,
    p_comment       VARCHAR(23) NOT NULL
) USING columnar;

CREATE TABLE supplier (
    s_suppkey       INT NOT NULL,
    s_name          CHAR(25) NOT NULL,
    s_address       VARCHAR(40) NOT NULL,
    s_nationkey     INT NOT NULL,
    s_phone         CHAR(15) NOT NULL,
    s_acctbal       DECIMAL(15,2) NOT NULL,
    s_comment       VARCHAR(101) NOT NULL
) USING columnar;

CREATE TABLE partsupp (
    ps_partkey      INT NOT NULL,
    ps_suppkey      INT NOT NULL,
    ps_availqty     INT NOT NULL,
    ps_supplycost   DECIMAL(15,2) NOT NULL,
    ps_comment      VARCHAR(199) NOT NULL
) USING columnar;

CREATE TABLE customer (
    c_custkey       INT NOT NULL,
    c_name          VARCHAR(25) NOT NULL,
    c_address       VARCHAR(40) NOT NULL,
    c_nationkey     INT NOT NULL,
    c_phone         CHAR(15) NOT NULL,
    c_acctbal       DECIMAL(15,2) NOT NULL,
    c_mktsegment    CHAR(10) NOT NULL,
    c_comment       VARCHAR(117) NOT NULL
) USING columnar;

CREATE TABLE orders (
    o_orderkey      BIGINT NOT NULL,
    o_custkey       INT NOT NULL,
    o_orderstatus   CHAR(1) NOT NULL,
    o_totalprice    DECIMAL(15,2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk         CHAR(15) NOT NULL,
    o_shippriority  INT NOT NULL,
    o_comment       VARCHAR(79) NOT NULL
) USING columnar;

CREATE TABLE lineitem (
    l_orderkey      BIGINT NOT NULL,
    l_partkey       INT NOT NULL,
    l_suppkey       INT NOT NULL,
    l_linenumber    INT NOT NULL,
    l_quantity      DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount      DECIMAL(15,2) NOT NULL,
    l_tax           DECIMAL(15,2) NOT NULL,
    l_returnflag    CHAR(1) NOT NULL,
    l_linestatus    CHAR(1) NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  CHAR(25) NOT NULL,
    l_shipmode      CHAR(10) NOT NULL,
    l_comment       VARCHAR(44) NOT NULL
) USING columnar;

CREATE TABLE nation (
    n_nationkey     INT NOT NULL,
    n_name          CHAR(25) NOT NULL,
    n_regionkey     INT NOT NULL,
    n_comment       VARCHAR(152) NOT NULL
) USING columnar;

CREATE TABLE region (
    r_regionkey     INT NOT NULL,
    r_name          CHAR(25) NOT NULL,
    r_comment       VARCHAR(152) NOT NULL
) USING columnar;

-- Distribute large tables (hashing on keys)
-- Co-locating critical joined tables to prevent cross-socket data shuffling!
SELECT create_distributed_table('part', 'p_partkey');
SELECT create_distributed_table('supplier', 's_suppkey');
SELECT create_distributed_table('partsupp', 'ps_partkey', colocate_with => 'part');
SELECT create_distributed_table('customer', 'c_custkey');
SELECT create_distributed_table('orders', 'o_orderkey');
SELECT create_distributed_table('lineitem', 'l_orderkey', colocate_with => 'orders');

-- Distribute small dimension tables as Reference Tables (fully replicated on all workers)
-- This enables fast, direct local joins on every worker node.
SELECT create_reference_table('nation');
SELECT create_reference_table('region');

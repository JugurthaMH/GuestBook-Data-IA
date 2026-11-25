import duckdb

con = duckdb.connect("warehouse.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

# 1. Clients : emails et pays normalisés
con.execute(r"""
    CREATE OR REPLACE TABLE silver.customers AS
    SELECT DISTINCT
        CAST(customer_id AS INTEGER) AS customer_id,
        trim(name)                   AS name,
                regexp_replace(regexp_replace(lower(trim(email)),'\.{2,}', '.', 'g'), '\.@', '@', 'g') AS email,
        upper(trim(country))         AS country
    FROM bronze.customers;
""")

# 2. Commandes : vraie date, montant en euros, sans doublons
con.execute(r"""
    CREATE OR REPLACE TABLE silver.orders AS
    SELECT DISTINCT
        CAST(order_id AS INTEGER)    AS order_id,
        CAST(customer_id AS INTEGER) AS customer_id,
        COALESCE(TRY_STRPTIME(date, '%d/%m/%Y'),
                 TRY_STRPTIME(date, '%Y-%m-%d'),
                 TRY_STRPTIME(date, '%Y%m%d')) AS order_date,
        CASE
            WHEN upper(trim(currency)) = 'USD' THEN CAST(CAST(amount AS DECIMAL(10, 2))
            * 0.9 AS DECIMAL(10, 2))
            WHEN upper(trim(currency)) = 'EUR' THEN CAST(amount AS DECIMAL(10, 2))
            ELSE NULL
        END AS amount_eur,
    FROM bronze.orders;
""")

print("Transform → silver done")

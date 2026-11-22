# Snowflake (step 10)

The gold star schema, loaded into a Snowflake trial account.

## One-time setup

1. Start a Snowflake trial (30 days). Note the account identifier
   (`<orgname>-<accountname>`, under your name → Account → View account details).
2. Choose how a script signs in:
   - at your desk: `SNOWFLAKE_AUTHENTICATOR=externalbrowser` (opens the browser);
   - for automation: key-pair authentication (`SNOWFLAKE_PRIVATE_KEY_FILE`), set up
     with `ALTER USER <you> SET RSA_PUBLIC_KEY = '...'`.
   Snowflake is phasing out password-only sign-in, so prefer these to
   `SNOWFLAKE_PASSWORD`. Keep all of them out of the repository.
3. `pip install -e ".[snowflake]"`

## Load

```bash
python -m ecom_etl run --source synthetic --no-load
python -m ecom_etl export-snowflake --source synthetic
export SNOWFLAKE_ACCOUNT=<orgname-accountname> SNOWFLAKE_USER=<you> SNOWFLAKE_AUTHENTICATOR=externalbrowser
python -m ecom_etl load-snowflake --source synthetic
```

`load-snowflake` creates (if missing) the warehouse `ECOM_WH` (X-Small,
suspends after 60 s idle), the database `ECOM`, the schema `GOLD`, a Parquet
file format and an internal stage; then for every gold table it replaces the
table, clears its stage folder, `PUT`s the exported files, runs `COPY INTO`
and compares `COUNT(*)` with the rows Spark wrote. It exits with code 1 if any
count differs. Names can be changed with `--database`, `--schema` and
`--warehouse`.

`export-snowflake` also writes the whole load as one script,
`reports/snowflake_<source>.sql`, to read or to run with the Snowflake CLI
(`snow sql -f ...`) or SnowSQL. `PUT` uploads local files, so the script does
not run in a Snowsight worksheet.

## Then

Run `queries.sql` in a worksheet, and suspend the warehouse when you are done:
`ALTER WAREHOUSE ECOM_WH SUSPEND;`

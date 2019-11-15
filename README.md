# Tim's ETL Tool

I wrote this helper tool to write insert scripts from Snowflake into Postgres. It needs a lot of work. I wrote it in
like 4 hours so give me a break.

## Getting Started
Make sure to activate virtual environment before running.

```
. venv/bin/activate
pip install -r requirements.txt
```

####::: You will need :::

```
SF_USER: snowflake username
SF_PASSWORD: snowflake password
SF_ACCOUNT: account (ie 'aj34234.us-east-1')
SF_WAREHOUSE: warehouse name
SF_DATABASE: database
SF_SCHEMA: schema

PG_USER: postgres username
PG_PASSWORD: password
PG_HOST: hostname database
PG_PORT: default "5432"
PG_DATABASE: database
```

###Config
Update the snowflake.sql file with the query that will retrieve from Snowflake

Update the columns.yml with tablename, columnnames, and constraint_key is you want to do upserts for TARGET table

```yaml
table:
  - tchang.account_test
columns:
  - additional_paid_collections
  - application
  - approvaldate_masked
  - bankruptcy_chapter
constraint_key:
  - account_pkey
```

## To Run CLI

```bash
tim_etl test
--will print the sql query
tim_etl run
--will actually run the inserts. BE CAREFUL
```

##Gotcha
* The columns order in the sql query and the yml file must be the same length and match perfectly.
* If tim_etl command doesn't work use "python main.py run/test"

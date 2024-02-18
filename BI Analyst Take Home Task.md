# BI Analyst Take Home Task

### Components Used
- Google BigQuery (SQL database)
- Looker Studio (BI)

### Repository Contents
- Overview text file
- deal_facts.sql
- deal_facts.csv

#### deal_facts.sql
This is the SQL used to build the deal_facts table from the deals and deal_pipeline_stages tables provided. 

deal_facts is created to view the pipeline stages, provide the most recent timestamps for when each deal entered the relevant 6 stages and other facts (eg, source type, monetary value) that may be used by Sales users from the [Looker Studio Sales Funnel Dashboard](https://lookerstudio.google.com/reporting/74ce6c00-6aaf-483a-add9-260f362f8d29).

It first finds the most recent timestamp for when a deal entered a stage, then denormalised the stage entry timestamps from the deal_pipeline stages table per deal. 

Following this, the rows are merged to remove nulls and retained in a STRUCT format for organisation. Dialect referenced from [this Stackoverflow article](https://stackoverflow.com/questions/57371692/bigquery-avoid-null-data-and-merge-rows). 

#### Assumptions Made

CRM Stages are static and unlikely to change.
HOME CURRENCY is already USD, (otherwise could look to integrate API Call eg(https://freecurrencyapi.com/) with exchange rate table).
Won CLosed Self-Serve is a different stage to Closed Won.

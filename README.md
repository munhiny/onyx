# onyx
onyx_challenge

## DBT Project Structure

This project is structured using the DBT framework. The project is divided into three layers: bronze, silver, and gold.

Bronze layer contains the raw data ingested from the CSV file. 

Silver layer contains the cleaned and validated data. This is originally SCD 1 data and so DBT snapshot is used to maintain history.  An incremental model is used to ingest the data for optimisation purposes incase volume of data increases.

Gold layer contains the business ready views as requested.  These were created as views as theses aggretations have a small volume of data and so the performance benefits of views are more significant.  We only create these views with the most current data.  Historical data is not stored in the gold layer.





## Setup Instructions

### 1. Poetry Installation & Dependencies
First, make sure you have Poetry installed:
```bash
brew install poetry
```

Navigate to the project directory:
```bash
cd onyx
```
Install dependencies anddbt Dependencies
```bash
cd dbt_project
poetry install
dbt deps
```
Generate own .env files with relevant database access config to postgres and place in root directory

```bash
touch .env
```

Example `.env` file:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=onyx
DB_USER=
DB_PASSWORD=
```


### 2. Run pipeline

The csv files should be in the root directory of the project. We can run a different csv file by passing the csv name to the pipeline.py file.

```bash
cd dbt_project
poetry run python pipeline.py -f "Data Engineer Challenge_input.csv"
```
### 3. DBT Docs

Generate dbt docs

```bash
poetry run python pipeline.py -g
```

Serve docs

```bash
poetry run python pipeline.py -s
```

## Data checks implemented

Uniqueness test on primary key columns - to check if there are any duplicates in the primary key columns

not null test on all primary key columns - to check if there are any nulls in the primary key columns

Date format test on bus_date column - to check if the bus_date column is in the correct date format

min value test on turnover_sum column - to check if the turnover_sum column is greater than or equal to 0

min value test on games_played_sum column - to check if the games_played_sum column is greater than or equal to 0


## Data Transformation 
Calculating the total turnover amount for each venue - sum turnover amount grouped by venue_code

Aggregating total revenue (gmp_sum) by EGM and by venue - sum revenue grouped by egm_description and venue_code

Creating a daily turnover and revenue summary - sum turnover and revenue grouped by bus_date


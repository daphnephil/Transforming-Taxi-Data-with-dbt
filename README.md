# Transforming Taxi Data with dbt

This repository contains a project that demonstrates the process of loading New York City taxi trips record data into Google BigQuery, transforming the data using dbt (data build tool), and creating visualizations using Looker. The project aims to showcase an end-to-end data pipeline for analyzing and visualizing taxi trip data.


![dbt-dag](https://github.com/daphnephil/Transforming-Taxi-Data-with-dbt/assets/62921301/cdb6bc9b-93d6-471d-a7fc-1d96c0b56e2a)

## Table of Contents

* Introduction
* Project-Structure
* Prerequisites
* Getting Started
* Loading Data into BigQuery
* Transforming Data with dbt
* Creating Visualizations with Looker
* Contributing

## Introduction
This project showcases the process of loading NYC taxi trip records into Google BigQuery, performing data transformations using dbt, and leveraging Looker for data visualization. The goal is to provide a comprehensive example of building a data pipeline for analyzing and gaining insights from taxi trip data.

## Project Structure
```
📦 
├─ .DS_Store
├─ README.md
├─ analyses
│  └─ .gitkeep
├─ dbt-dag.png
├─ dbt_project.yml
├─ macros
│  └─ get_payment_type_description.sql
├─ models
│  ├─ core
│  │  ├─ dim_zones.sql
│  │  ├─ dm_monthly_zone_revenue.sql
│  │  ├─ fact_fhv_trips.sql
│  │  ├─ fact_trips.sql
│  │  └─ schema.yml
│  └─ staging
│     ├─ schema.yml
│     ├─ stg_fhv_tripsdata.sql
│     ├─ stg_green_tripdata.sql
│     └─ stg_yellow_tripdata.sql
├─ package-lock.yml
├─ packages.yml
├─ seeds
│  ├─ .gitkeep
│  └─ taxi_zone_lookup.csv
└─ snapshots
   └─ .gitkeep
```



## Prerequisites
Before getting started, ensure that you have the following prerequisites installed:

* Python
* Mage
* dbt
* Google Cloud SDK (for BigQuery)
* Looker (optional, for visualization)

## Getting Started
1. Clone the repository:
```
git clone https://github.com/daphnephil/Transforming-Taxi-Data-with-dbt.git
cd Transforming-Taxi-Data-with-dbt
```


## Loading Data into BigQuery
...

## Transforming Data with dbt
...

## Creating Visualizations with Looker


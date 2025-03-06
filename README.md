# Covid19-ETL

This project is an ETL (Extract, Transform, Load) pipeline that extracts COVID-19 data from a public source, processes it, and loads it into a PostgreSQL database. The data is organized into dimension and fact tables to enable efficient querying and analysis.

![Diagrams](https://github.com/user-attachments/assets/30f23e9f-138a-4414-99cc-9dfac22dd5dc)

## Table of Contents

- [Overview](#overview)
- [Technologies](#technologies)
- [Workflow](#workflow)
- [Data Pipeline Tasks](#data-pipeline-tasks)
- [Database Schema](#database-schema)
- [Challenges and Solutions](#challenges-and-solutions)
- [Future Enhancements](#future-enhancements)

## Overview

The pipeline retrieves daily COVID-19 data from the [Johns Hopkins University GitHub repository](https://github.com/CSSEGISandData/COVID-19), processes it, and stores it in a PostgreSQL database. The data includes information such as the number of confirmed cases, deaths, recoveries, and geographical details.

## Technologies

- **Python**: Core programming language for data processing.
- **Apache Airflow**: Orchestrates the ETL pipeline.
- **Pandas**: Data manipulation and transformation.
- **PostgreSQL**: Destination database for storing processed data.
- **Docker**: Containerization and environment consistency.

## Workflow

1. **Data Extraction**: Fetches CSV files for daily COVID-19 reports.
2. **Data Transformation**: Cleans and processes the data to standardize formats and derive additional attributes.
3. **Data Loading**: Loads the data into PostgreSQL tables using batch inserts.

## Data Pipeline Tasks

### 1. Create Tables

Three tables are created:
- `dim_date`: Stores date-related attributes.
- `dim_location`: Stores location details such as region, state, latitude, and longitude.
- `fact_covid_cases`: Stores COVID-19 case details (cases, deaths, recoveries) along with references to `dim_date` and `dim_location`.

### 2. Extract Data

The pipeline fetches data from the public GitHub repository for the specified dates and years. Invalid or missing files are logged for review.

### 3. Transform Data

Transformations include:
- Standardizing column names.
- Handling missing values.
- Adding derived columns like `date_id` and `location_id`.

### 4. Load Data

Data is inserted into the database in batches to handle large datasets efficiently. Conflict handling ensures no duplicate records are inserted.

## Database Schema

### Dimension Tables

#### `dim_date`
| Column       | Type    | Description             |
|--------------|---------|-------------------------|
| year         | INTEGER | Year                   |
| month        | INTEGER | Month                  |
| month_name   | VARCHAR | Name of the month      |
| day          | INTEGER | Day of the month       |
| quarter      | INTEGER | Quarter of the year    |
| date_id      | INTEGER | Primary key (YYYYMMDD) |

#### `dim_location`
| Column      | Type             | Description            |
|-------------|------------------|------------------------|
| region      | VARCHAR          | Country/region name    |
| state       | VARCHAR          | State/province name    |
| lat         | DOUBLE PRECISION | Latitude coordinate    |
| long        | DOUBLE PRECISION | Longitude coordinate   |
| location_id | VARCHAR          | Primary key (unique ID)|

### Fact Table

#### `fact_covid_cases`
| Column      | Type    | Description                              |
|-------------|---------|------------------------------------------|
| date_id     | INTEGER | Foreign key referencing `dim_date`       |
| location_id | VARCHAR | Foreign key referencing `dim_location`   |
| cases       | INTEGER | Number of confirmed cases                |
| deaths      | INTEGER | Number of deaths                         |
| recoveries  | INTEGER | Number of recoveries                     |
| case_id     | SERIAL  | Primary key                              |

## Challenges and Solutions

### 1. **Missing or Invalid Data**
- **Problem**: Some daily reports were missing or contained invalid data.
- **Solution**: Implemented error handling to skip invalid files and log errors for review.

### 2. **Duplicate Records**
- **Problem**: Duplicate records caused by reprocessing the same data.
- **Solution**: Added `ON CONFLICT DO NOTHING` in SQL inserts to prevent duplication.

### 3. **Batch Processing**
- **Problem**: Large datasets caused performance bottlenecks.
- **Solution**: Implemented batch inserts using `execute_batch` to improve efficiency.

### 4. **Date Validation**
- **Problem**: Invalid date formats in input files.
- **Solution**: Used Python's `datetime` module to validate and parse dates.

## Future Enhancements

- Automate error handling to retry failed extractions.
- Implement data validation checks post-loading.
- Use Airflow's UI to monitor pipeline metrics and performance.
- Add visualization dashboards for real-time insights.


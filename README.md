cat > README.md <<'EOF'
# Day 21 - Spark Catalog Practice

## Overview

This project demonstrates Apache Spark Catalog and Spark SQL capabilities using a small hotel booking analytics scenario.

The application demonstrates:

- Listing databases
- Creating and using databases
- Creating temporary views
- Listing tables and views
- Registering DataFrames as managed tables
- Querying registered tables using Spark SQL
- Inspecting table metadata
- Inspecting schema metadata
- Using `DESCRIBE TABLE`
- Using `DESCRIBE EXTENDED`
- Performing hotel booking analytics

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.1
- Spark SQL
- sbt 2.0.8
- Java 17
- WSL2 / Linux

## Project Structure

```text
Day21-Catalog-Practice/
├── build.sbt
├── README.md
├── .gitignore
├── project/
└── src/
    └── main/
        └── scala/
            └── com/
                └── day21catalog/
                    └── HotelCatalogApp.scala
Scenario

The project simulates a small hotel booking analytics system.

The booking dataset contains:

Booking ID
Customer Name
Hotel
Room Type
Number of Nights
Booking Amount
Booking Status

Example hotels:

Grand Palace
Sea View
Mountain Resort

Example room types:

Standard
Deluxe
Suite
Spark Catalog Operations
List Databases
spark.catalog.listDatabases().show(false)
Create Database
CREATE DATABASE IF NOT EXISTS hotel_analytics
Use Database
USE hotel_analytics
Create Temporary View
bookingDF.createOrReplaceTempView("booking_view")

The temporary view can then be queried using Spark SQL.

List Tables and Views
spark.catalog.listTables().show(false)

This shows both temporary views and registered tables.

Register DataFrame as Table
bookingDF.write
  .mode("overwrite")
  .saveAsTable("hotel_bookings")
Query Registered Table
SELECT
    hotel,
    SUM(amount) AS total_revenue,
    SUM(nights) AS total_nights
FROM hotel_bookings
WHERE status = 'CONFIRMED'
GROUP BY hotel
ORDER BY total_revenue DESC
Metadata Inspection
Table Metadata
spark.catalog.getTable("hotel_bookings")

The application displays:

Table name
Database
Table type
Temporary status
Column Metadata
spark.catalog.listColumns("hotel_bookings").show(false)
Describe Table
DESCRIBE hotel_bookings
Detailed Table Information
DESCRIBE EXTENDED hotel_bookings

This provides information such as:

Catalog
Database
Table
Creation time
Spark version
Table type
Storage provider
Table location
Analytics Results

The application analyzes confirmed hotel bookings.

Revenue by Hotel
Hotel	Total Revenue	Total Nights
Sea View	1850.0	7
Grand Palace	950.0	8
Revenue by Hotel and Room Type
Hotel	Room Type	Bookings	Nights	Revenue	Average Booking
Sea View	Suite	2	7	1850.0	925.0
Grand Palace	Standard	2	5	500.0	250.0
Grand Palace	Deluxe	1	3	450.0	450.0

Cancelled bookings are excluded from the final confirmed-booking analytics.

Temporary View vs Managed Table
Temporary View

booking_view

Created from a DataFrame
Available during the Spark session
Not a permanent table
Useful for temporary SQL analysis
Managed Table

hotel_analytics.hotel_bookings

Registered in the Spark catalog
Stored as a managed table
Can be queried using Spark SQL
Has persistent table metadata
How to Run

From the project directory:

cd ~/Day21-Catalog-Practice

Run the application:

sbt run

The application should finish with:

========================================
       DAY 21 COMPLETED SUCCESSFULLY
========================================
Key Concepts Learned
Spark Catalog

Spark Catalog provides APIs for interacting with databases, tables, views, and metadata.

Temporary Views

Temporary views allow DataFrames to be queried using SQL without creating a permanent table.

Managed Tables

Managed tables are registered in the Spark catalog and have associated storage managed by Spark.

Metadata

Metadata describes the structure and properties of databases, tables, columns, and storage.

Spark SQL

Spark SQL provides a SQL interface for querying DataFrames, views, and tables.

Conclusion

This project demonstrates how Spark Catalog can be used to manage and inspect a small hotel analytics environment.

The project successfully covers database creation, temporary views, table registration, SQL querying, schema inspection, metadata inspection, and hotel booking analytics.
EOF

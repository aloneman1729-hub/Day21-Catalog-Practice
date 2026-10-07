package com.day21catalog

import org.apache.spark.sql.SparkSession

object HotelCatalogApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hotel Catalog Practice")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    println("\n========================================")
    println("   DAY 21 - SPARK CATALOG PRACTICE")
    println("========================================\n")

    // --------------------------------------------------
    // 1. List existing databases
    // --------------------------------------------------

    println("1. EXISTING DATABASES")
    println("---------------------")

    spark.catalog.listDatabases().show(false)


    // --------------------------------------------------
    // 2. Create hotel analytics database
    // --------------------------------------------------

    println("2. CREATING HOTEL ANALYTICS DATABASE")
    println("-------------------------------------")

    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")

    spark.catalog.listDatabases().show(false)


    // --------------------------------------------------
    // 3. Use the database
    // --------------------------------------------------

    println("3. USING HOTEL_ANALYTICS DATABASE")
    println("---------------------------------")

    spark.sql("USE hotel_analytics")

    println("Current database: " + spark.catalog.currentDatabase)


    // --------------------------------------------------
    // 4. Create hotel booking DataFrame
    // --------------------------------------------------

    println("\n4. CREATING HOTEL BOOKING DATA")
    println("------------------------------")

    val bookings = Seq(
      (1001, "Prudhvi", "Grand Palace", "Deluxe", 3, 450.0, "CONFIRMED"),
      (1002, "Rahul", "Sea View", "Suite", 2, 600.0, "CONFIRMED"),
      (1003, "Ananya", "Grand Palace", "Standard", 4, 400.0, "CONFIRMED"),
      (1004, "Kiran", "Mountain Resort", "Deluxe", 2, 500.0, "CANCELLED"),
      (1005, "Sneha", "Sea View", "Suite", 5, 1250.0, "CONFIRMED"),
      (1006, "Arjun", "Grand Palace", "Standard", 1, 100.0, "CONFIRMED")
    )

    val bookingDF = bookings.toDF(
      "booking_id",
      "customer_name",
      "hotel",
      "room_type",
      "nights",
      "amount",
      "status"
    )

    bookingDF.show(false)


    // --------------------------------------------------
    // 5. Create temporary view
    // --------------------------------------------------

    println("\n5. CREATING TEMPORARY VIEW")
    println("-------------------------")

    bookingDF.createOrReplaceTempView("booking_view")

    println("Temporary view 'booking_view' created.")


    // --------------------------------------------------
    // 6. List tables and views
    // --------------------------------------------------

    println("\n6. TABLES AND VIEWS IN CATALOG")
    println("------------------------------")

    spark.catalog.listTables().show(false)


    // --------------------------------------------------
    // 7. Query temporary view
    // --------------------------------------------------

    println("\n7. QUERYING TEMPORARY VIEW")
    println("--------------------------")

    spark.sql(
      """
        |SELECT hotel, COUNT(*) AS total_bookings
        |FROM booking_view
        |GROUP BY hotel
        |ORDER BY total_bookings DESC
        |""".stripMargin
    ).show(false)


    // --------------------------------------------------
    // 8. Register DataFrame as permanent table
    // --------------------------------------------------

    println("\n8. REGISTERING DATAFRAME AS TABLE")
    println("---------------------------------")

    bookingDF.write
      .mode("overwrite")
      .saveAsTable("hotel_bookings")

    println("Table 'hotel_bookings' created.")


    // --------------------------------------------------
    // 9. List tables again
    // --------------------------------------------------

    println("\n9. LISTING TABLES AFTER REGISTRATION")
    println("-----------------------------------")

    spark.catalog.listTables().show(false)


    // --------------------------------------------------
    // 10. Query registered table
    // --------------------------------------------------

    println("\n10. QUERYING REGISTERED TABLE")
    println("-----------------------------")

    spark.sql(
      """
        |SELECT
        |  hotel,
        |  SUM(amount) AS total_revenue,
        |  SUM(nights) AS total_nights
        |FROM hotel_bookings
        |WHERE status = 'CONFIRMED'
        |GROUP BY hotel
        |ORDER BY total_revenue DESC
        |""".stripMargin
    ).show(false)


    // --------------------------------------------------
    // 11. Inspect table metadata
    // --------------------------------------------------

    println("\n11. TABLE METADATA")
println("------------------")

val tableMetadata = spark.catalog.getTable("hotel_bookings")

println(s"Table name       : ${tableMetadata.name}")
println(s"Database         : ${tableMetadata.database}")
println(s"Table type       : ${tableMetadata.tableType}")
println(s"Is temporary     : ${tableMetadata.isTemporary}")
    // --------------------------------------------------
    // 12. Inspect schema metadata
    // --------------------------------------------------

    println("\n12. SCHEMA METADATA")
    println("-------------------")

    spark.catalog.listColumns("hotel_bookings").show(false)


    // --------------------------------------------------
    // 13. DESCRIBE TABLE
    // --------------------------------------------------

    println("\n13. DESCRIBE TABLE")
    println("------------------")

    spark.sql("DESCRIBE hotel_bookings").show(false)


    // --------------------------------------------------
    // 14. Detailed table information
    // --------------------------------------------------

    println("\n14. DETAILED TABLE INFORMATION")
    println("------------------------------")

    spark.sql("DESCRIBE EXTENDED hotel_bookings").show(false)


    // --------------------------------------------------
    // 15. Final analytics query
    // --------------------------------------------------

    println("\n15. HOTEL BOOKING ANALYTICS")
    println("--------------------------")

    spark.sql(
      """
        |SELECT
        |  hotel,
        |  room_type,
        |  COUNT(*) AS bookings,
        |  SUM(nights) AS total_nights,
        |  SUM(amount) AS revenue,
        |  AVG(amount) AS average_booking_amount
        |FROM hotel_bookings
        |WHERE status = 'CONFIRMED'
        |GROUP BY hotel, room_type
        |ORDER BY revenue DESC
        |""".stripMargin
    ).show(false)


    println("\n========================================")
    println("       DAY 21 COMPLETED SUCCESSFULLY")
    println("========================================\n")

    spark.stop()
  }
}

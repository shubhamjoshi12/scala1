import org.apache.spark.sql.functions.{col, count, initcap, sum, when}

object question3 {
  import org.apache.spark.sql.SparkSession

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()

      .appName("newapp")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
      ("neha", "ProjectC", 80),
      ("neha", "ProjectD", 30),
      ("priya", "ProjectE", 110),
      ("mohan", "ProjectF", 40),
      ("ajay", "ProjectG", 70),
      ("vijay", "ProjectH", 150),
      ("veer", "ProjectI", 190),
      ("aatish", "ProjectJ", 60),
      ("animesh", "ProjectK", 95),
      ("nishad", "ProjectL", 210),
      ("varun", "ProjectM", 50),
      ("aadil", "ProjectN", 90)
    ).toDF("name", "project", "hours")

    val agg_df=workload.groupBy(col("name")).agg(sum("hours").alias("total_hours"))
//    agg_df.show()

   val new_df=agg_df.select(initcap(col("name")).alias("name"),col("total_hours"),when(col("total_hours")>200,"Overloaded")
   .when((col("total_hours")>=100)&&(col("total_hours")<=200),"Balanced")
     .when(col("total_hours")<100,"Underutilized")
     .otherwise("Not categorized").alias("Category")
   )
//    new_df.show()

    val category_df=new_df.groupBy("Category").agg(count("name").alias("count"))

//    category_df.show()


    workload.createOrReplaceTempView("employee")
    val sql_df=spark.sql(
      """
         select INITCAP(name) as name, SUM(hours) AS total_hours,case
         when sum(hours)>200 then 'Overloaded'
         when sum(hours)>=100 and sum(hours)<=200 then 'Balanced'
         when sum(hours)<200 then 'Underutilized'
         end as Category
         from employee
         group by name

        """)
    sql_df.show()
    sql_df.createOrReplaceTempView("table2")
    val sql_new_df=spark.sql(
      """
        select category ,count(name) from table2 group by category
      """)
    sql_new_df.show()


  }

}

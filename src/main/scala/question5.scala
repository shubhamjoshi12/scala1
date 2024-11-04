import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, sum, when,count}


object question5 {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("newapp")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val employees = List(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    ).toDF("name", "hours_worked")

    val df=employees.select(initcap(col("name")).alias("name"),col("hours_worked"),when(col("hours_worked")>60,"Excessive Overtime")
    .when((col("hours_worked")>=45)&&(col("hours_worked")<=60),"Standard Overtime")
      .when(col("hours_worked")<45,"No overtime")
      .otherwise("Not categorized").alias("Category")
    )
//    df.show()
    val final_df=df.groupBy("Category").agg(count("name").alias("count"))
    final_df.show()
    employees.createOrReplaceTempView("emp")
    val df2=spark.sql(
      """
       select initcap(name) as name , hours_worked, case
       when hours_worked>60 then 'Excessive Overtime'
       when hours_worked>=45 and hours_worked<=60 then 'Standard Overtime'
       when hours_worked<45 then 'no overtime'
       else 'not categorized'
       end as category
       from emp """)
    df2.show()

    df2.createOrReplaceTempView("final")
    val df3=spark.sql(
      """
         select category, count(*) from final group by category

        """)
    df3.show()



  }
}
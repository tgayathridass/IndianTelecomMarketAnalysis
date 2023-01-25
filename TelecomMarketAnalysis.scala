//Analyze customer sentiment towards different telecommunication companies in India.
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.sum

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.TimestampType


object IndianTelecomAnalysis extends App {
    
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .appName("IndianTelecomAnalysis")
  .master("local[2]")
  .enableHiveSupport()
  .getOrCreate()
  
  val inputfile = spark.read.format("csv")
                  .option("header",true)
                  .option("inferSchema",true)
                  .option("path","C:/Users/Documents/Indian telecommunications market.csv")
                  .load
   inputfile.show()
   
   // As column names is too big, renamed the columns
   //method:1 to rename
   
   inputfile.withColumnRenamed("Timestamp","Q1")
            .withColumnRenamed("1.Which region of India are you from?","Q2")
            .withColumnRenamed("2.Which type of location do you live in?","Q3")
            .withColumnRenamed("3.Which tech are you currently using?","Q4")
            .withColumnRenamed("4.Can you send an email with no attachments and just paragraphs of text instantly?","Q5")
            .withColumnRenamed("5.Are one-on-one video calls a smooth experience?","Q6")
            .withColumnRenamed("6.Do you experience delays in video conference calls?","Q7")
            .withColumnRenamed("7.How long does it take you to download a song of 4 minutes?","Q8")
            .withColumnRenamed("8.How much data do you use each month?","Q9")
            .withColumnRenamed("9.What do you usually do with your phone? (Multiple choices)","Q10")
            .withColumnRenamed("10.What do you think about the call drop rate? ","Q11")
            .withColumnRenamed("11.Which network are you likely to recommend to a tourist who is in town for a few days?","Q12")
            .withColumnRenamed("12.Which network do you use more often?","Q13")
            .withColumnRenamed("13-VI.Why do you use your current network?","Q14")
            .withColumnRenamed("14-VI.How much are you paying for your mobile bill & WiFi services per month currently?","Q15")
            .withColumnRenamed("15-VI.If Airtel lowers its price a little, or if Jio provides higher quality services, will you switch?","Q16")
            .withColumnRenamed("16-VI.Rate the service quality","Q17")
            .withColumnRenamed("13-Jio.Why do you use your current network?","Q18")
            .withColumnRenamed("14-Jio.How much are you paying for your mobile bill & WiFi services per month currently?","Q19")
            .withColumnRenamed("15-Jio.If VI lowers its price for services to the same level as Jio, will you switch to VI?","Q20")
            .withColumnRenamed("16-Jio.Rate the service quality","Q21")
            .withColumnRenamed("13-Airtel.Why do you use your current network?","Q22")
            .withColumnRenamed("14-Airtel.How much are you paying for your mobile bill & WiFi services per month currently?","Q23")
            .withColumnRenamed("15-Airtel.If VI lowers its price for services to the same level as Airtel, will you switch to VI?","Q24")
            .withColumnRenamed("16-Airtel.If VI provides higher quality services with a higher price as compared to Airtel, will you switch to VI?","Q25")
            .withColumnRenamed("17-Airtel.Rate the service quality","Q26")
            .withColumnRenamed("18.How much support do you think these companies are obtaining from the government?","Q27")
            .withColumnRenamed("19.Do you think the telecommunications market in India is tilting towards a monopoly?","Q28")
            
       //method 2: to rename     
      val newColumns = Seq("Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10","Q11","Q12","Q13","Q14","Q15","Q16","Q17","Q18","Q19","Q20","Q21","Q22","Q23","Q24","Q25","Q26","Q27","Q28")
      val newinput = inputfile.toDF(newColumns:_*)
      
      //total no of rows and columns  
      val total_rows = newinput.count()
      val total_columns = newinput.columns.length
      println("total no of columns: "+total_columns)
      println("total no of rows: "+total_rows)
   
   //Seggregate numerical  and categorical values  
   val drpCols = Seq("Q2","Q3","Q4","Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q12", "Q13", "Q14", "Q15", "Q16", "Q18", "Q19", "Q20", "Q22", "Q23", "Q24" ,"Q25", "Q28" )
   val numdf = newinput.drop(drpCols:_*)
   
   val drpCols1 = Seq("Q1","Q11","Q17","Q21","Q26","Q27")
   val catgrydf = newinput.drop(drpCols1:_*)
   
   val updateColname = Seq("Timestamp","Call_drop_rate","VI_Rating","JIO_Rating","Airtel_Rating","Govt_Supp")
   val Analzdf = numdf.toDF(updateColname:_*)
   Analzdf.show()    
     
   //Total null values in dataset
   val result = newinput.select( 
    newinput.columns.map(x => sum(isnull(col(x)) cast "int"))
      // computing the sum
      .reduce(_+_) as "total_nulls")
      
      println("Total nulls in the datasert") 
      result.show()
 
      // duplicate count
     val dupcnt =  newinput
                  .groupBy("Q1")
                  .count
                  .filter("count > 1")
                  .count()
     println("total duplicates count: "+dupcnt) 
  
  //Categorical Data   
   println("Which type of region does customer lives ") 
  newinput.createOrReplaceTempView("input_data") 
  spark.sql("select  Q3 from input_data")
 .groupBy("Q3")
 .count()
 .show(50)  
       
 println("How long does it take to download a 4 minutes song ") 
 newinput.createOrReplaceTempView("input_data") 
  spark.sql("select  Q8, Q13 from input_data")
 .groupBy("Q8")
 .pivot("Q13")
 .count()
 .show(50)  
  
   println("Are one on one video conference a smooth experience ") 
  newinput.createOrReplaceTempView("input_data") 
  spark.sql("select  Q6 from input_data")
 .groupBy("Q6")
 .count()
 .show(50) 
 
   println("Which network are you likely to recommend to a tourist who is in town for a few days?") 
  newinput.createOrReplaceTempView("input_data") 
  spark.sql("select  Q12 from input_data")
 .groupBy("Q12")
 .count()
 .show(50) 
 
 println("Monopoly in India")
  spark.sql("select  Q12, Q28 from input_data")
  .groupBy("Q12")
  .pivot("Q28")
  .count()
  .show(50)
 
   spark.stop()
  
}

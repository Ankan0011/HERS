package com.hers.rawstage
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import java.util.Date
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.hadoop.fs._
import scala.collection.Seq
import java.net.URI
import com.hers.rawstage.CreateDF
import com.hers.utility.Property


object hersraw {

    def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "C:\\SparkDev");
    //System.setProperty("spark.sql.warehouse.dir", "C:\\spark-warehouse");
    
     /**
     * Spark configuration setup with serializer
     */
    //val sparkconf = new SparkConf().setAppName("HERS").setMaster("local")
    
 
    val sparkconf = new SparkConf().setAppName("HERS")
    sparkconf.set("org.apache.spark.serializer.KryoSerializer", "spark_serializer")
    val sc = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //Loading the CHEERS & CalCERT files into dataframe
    val calCERT_file = Property.getProperty("calCERT_file")
    val cheers_file = Property.getProperty("cheers_file")
    /*
    val calCERT_file = "C:\\CEC_Documents\\HERS\\CalCERTS.csv"
    val cheers_file = "C:\\CEC_Documents\\HERS\\CHEERS.csv"
    */
    //Paths for Output files in Curated folder
    val clean_data= Property.getProperty("clean_data")
    val clean_data_cheers = Property.getProperty("clean_data_cheers")
    val error_data_cheers = Property.getProperty("error_data_cheers")
    val error_data_calCERT = Property.getProperty("error_data_calCERT")
    val temp_folder = Property.getProperty("temp_folder")
    
    /*
    val clean_data= "C:\\CEC_Documents\\HERS\\Curated\\calCERT\\Clean\\temp\\Curated_clean.csv"
    val clean_data_cheers = "C:\\CEC_Documents\\HERS\\Curated\\cheers\\Clean\\temp\\Curated_clean.csv"
    val error_data_cheers = "C:\\CEC_Documents\\HERS\\Curated\\cheers\\Error\\Curated_cheers_error.csv"
    val error_data_calCERT = "C:\\CEC_Documents\\HERS\\Curated\\calCERT\\Error\\Curated_calCERT_error.csv"
    val temp_folder = "C:\\CEC_Documents\\HERS\\Curated\\temp"
    val temp_folder_error = "C:\\CEC_Documents\\HERS\\Curated\\calCERT\\Error\\temp"
    */
    val DF: CreateDF = new CreateDF()
    
    //Loading the CIS file into a dataframe
    val calCERT_df_temp = DF.CreateDF(calCERT_file, ",", sqlContext).withColumnRenamed("CECRegistration_full", "Registration_Number")
    val cheers_df_temp = DF.CreateDF(cheers_file, ",", sqlContext).withColumnRenamed("RegistrationNumber", "Registration_Number")
    
    //Join between CHEERS & CalCERT data
    //val Combine_DF = calCERT_df.join(cheers_df , Seq("Registration_Number"))
    
    //User defined function to split the registration ID
    val zip = udf((t1: String) => if (t1 != null) t1.dropRight(1) else "")
    val zip02 = udf((t1: String) => if (t1 != null) t1.substring(t1.length()-1) else "")
    val zip03 = udf((t1: String, t2: String) => if (t1 != null && t2 != null) t1+t2 else "")
    val zip04 = udf((t1: String, t2: String) => if (t1.length() == 0) t2 else t1)
    
    val ignoreNullNA = udf((t1: String) => if (t1 == "NULL" || t1 == "Not Available") "" else t1)
    val ignoreNA = udf((t1: String) => if (t1 == "Not Available") "" else t1)
    
    val calCERT_df_pro = calCERT_df_temp.withColumn("Registration_Number", ignoreNullNA(col("Registration_Number"))).withColumn("Buildin Department",ignoreNullNA(col("Buildin Department"))).withColumn("lot City",ignoreNullNA(col("lot City"))).withColumn("lot Zip",ignoreNullNA(col("lot Zip")))
    .withColumn("Builder", ignoreNullNA(col("Builder"))).withColumn("Contractor", ignoreNullNA(col("Contractor"))).withColumn("Rater Company", ignoreNullNA(col("Rater Company"))).withColumn("Docauthor Company", ignoreNullNA(col("Docauthor Company"))).withColumn("A07-Refrigerant type", ignoreNullNA(col("A07-Refrigerant type")))
    .withColumn("F05-Liquid Line Pressure", ignoreNA(col("F05-Liquid Line Pressure"))).withColumn("F06-Condenser Saturation Temperature", ignoreNA(col("F06-Condenser Saturation Temperature"))).withColumn("G02-Suction Line Pressure", ignoreNA(col("G02-Suction Line Pressure"))).withColumn("G03-Evaporator Saturation Temperature", ignoreNA(col("G03-Evaporator Saturation Temperature")))
    
    val cheers_df = cheers_df_temp.withColumn("Registration_Number", ignoreNullNA(col("Registration_Number"))).withColumn("EnforcementAgency", ignoreNullNA(col("EnforcementAgency"))).withColumn("City", ignoreNullNA(col("City"))).withColumn("AuthorCompany", ignoreNullNA(col("AuthorCompany")))
    .withColumn("InstallerCompany", ignoreNullNA(col("InstallerCompany"))).withColumn("RaterCompany", ignoreNullNA(col("RaterCompany"))).withColumn("A07", ignoreNullNA(col("A07"))).withColumn("ZipCode", ignoreNA(col("ZipCode"))).withColumn("F05", ignoreNA(col("F05"))).withColumn("F06", ignoreNA(col("F06")))
    .withColumn("G02", ignoreNA(col("G02"))).withColumn("G03", ignoreNA(col("G03")))
    
    val calCERT_df = calCERT_df_pro.withColumnRenamed("Builder", "Installer Company").withColumn("Installer Company", zip04(col("Installer Company"),col("Contractor"))).drop("Contractor")
    
    //cheers_df.groupBy("Registration_Number").agg(count(col("Registration_Number")).alias("Count")).filter("Count > 1").show()
    
    //Creating extra columns to perform aggregation to capture serial registration ID
    val df = calCERT_df.withColumn("Section01", zip(col("Registration_Number"))).withColumn("Section02", zip02(col("Registration_Number")))
    //Aggregated dataframe with series
    val aggegatedDF_temp = df.as("D1").groupBy("Section01").agg(count(col("Section01")).alias("Total_Count"), max(col("Section02")).alias("Max")).filter("Total_Count > 1")
    var aggegatedDF = sqlContext.createDataFrame(sc.emptyRDD[Row],aggegatedDF_temp.schema )
    if (!aggegatedDF_temp.limit(1).rdd.isEmpty){
    aggegatedDF = aggegatedDF_temp.as("D2").withColumn("Registration_Number", zip03(col("Section01"), col("Max"))).drop("Section01").drop("Total_Count").drop("Max")
    }
    //Aggregated dataframe with no series
    val aggegatedDF_temp02 = df.as("D1").groupBy("Section01").agg(count(col("Section01")).alias("Total_Count"), max(col("Section02")).alias("Max")).filter("Total_Count = 1")
    val aggegatedDF02 = aggegatedDF_temp02.as("D3").withColumn("Registration_Number", zip03(col("Section01"), col("Max"))).drop("Section01").drop("Total_Count").drop("Max")

    
    //Clean DF is obtained by joining dataframe with no series & max value of series
    val clean_DF = aggegatedDF02.join(calCERT_df,  Seq("Registration_Number"))
    var clean_DF_part2 = sqlContext.createDataFrame(sc.emptyRDD[Row],clean_DF.schema )
    if (!aggegatedDF_temp.limit(1).rdd.isEmpty){
    clean_DF_part2 = aggegatedDF.join(calCERT_df,  Seq("Registration_Number"))
    }
    
    
    //Clean data is obtained by union dataframe with no series & max value of series
    val clean_final = clean_DF.unionAll(clean_DF_part2)
    
    //Error data is obtained by subtracting clean from error
    val Error_DF = calCERT_df.except(clean_final)
    
        
     //Creating extra columns to perform aggregation to capture serial registration ID
    val df01 = cheers_df.withColumn("Section01", zip(col("Registration_Number"))).withColumn("Section02", zip02(col("Registration_Number")))
    //Aggregated dataframe with series
    val aggegatedDF_temp_cheers = df01.as("D4").groupBy("Section01").agg(count(col("Section01")).alias("Total_Count"), max(col("Section02")).alias("Max")).filter("Total_Count > 1")
    var aggegatedDF_cheers = sqlContext.createDataFrame(sc.emptyRDD[Row],aggegatedDF_temp_cheers.schema )
    if (!aggegatedDF_temp_cheers.limit(1).rdd.isEmpty){
    aggegatedDF_cheers = aggegatedDF_temp_cheers.as("D5").withColumn("Registration_Number", zip03(col("Section01"), col("Max"))).drop("Section01").drop("Total_Count").drop("Max")
    }
    //Aggregated dataframe with no series
    val aggegatedDF_temp02_cheers = df01.as("D6").groupBy("Section01").agg(count(col("Section01")).alias("Total_Count"), max(col("Section02")).alias("Max")).filter("Total_Count = 1")
    val aggegatedDF02_cheers = aggegatedDF_temp02_cheers.as("D7").withColumn("Registration_Number", zip03(col("Section01"), col("Max"))).drop("Section01").drop("Total_Count").drop("Max")

    
    //Clean DF is obtained by joining dataframe with no series & max value of series
    val clean_DF_cheers = aggegatedDF02_cheers.join(cheers_df,  Seq("Registration_Number"))
    var clean_DF_part2_cheers = sqlContext.createDataFrame(sc.emptyRDD[Row],clean_DF_cheers.schema )
    if (!aggegatedDF_temp.limit(1).rdd.isEmpty){
    clean_DF_part2_cheers = aggegatedDF_cheers.join(cheers_df,  Seq("Registration_Number"))
    }
    
    
    //Clean data is obtained by union dataframe with no series & max value of series
    val clean_final_cheers = clean_DF_cheers.unionAll(clean_DF_part2_cheers)
    
    //Error data is obtained by subtracting clean from error
    val Error_DF_cheers = cheers_df.except(clean_final_cheers)
   
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
   
    //Writing the files in the mentioned path and renaming the file in csv format
    hdfs.delete(new Path(temp_folder), true)
    clean_final.repartition(1).write.format("com.databricks.spark.csv").mode("append").option("delimiter", "\t").option("header", "true").save(temp_folder)
    hdfs.rename(new Path(temp_folder+"/part-00000"), new Path(clean_data))
    hdfs.delete(new Path(temp_folder), true)

    clean_final_cheers.repartition(1).write.format("com.databricks.spark.csv").mode("append").option("delimiter", "\t").option("header", "true").save(temp_folder)
    hdfs.rename(new Path(temp_folder+"/part-00000"), new Path(clean_data_cheers))
    hdfs.delete(new Path(temp_folder), true)

    
    Error_DF.repartition(1).write.format("com.databricks.spark.csv").mode("append").option("delimiter", "\t").option("header", "true").save(temp_folder)
    hdfs.rename(new Path(temp_folder+"/part-00000"), new Path(error_data_calCERT))
    hdfs.delete(new Path(temp_folder), true)
    
    Error_DF_cheers.repartition(1).write.format("com.databricks.spark.csv").mode("append").option("delimiter", "\t").option("header", "true").save(temp_folder)
    hdfs.rename(new Path(temp_folder+"/part-00000"), new Path(error_data_cheers))
    hdfs.delete(new Path(temp_folder), true)
    
     
  }

}
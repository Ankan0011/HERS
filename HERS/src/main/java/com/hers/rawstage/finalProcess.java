package com.hers.rawstage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import com.hers.utility.Property;



public class finalProcess {

	public static void main(String[] args)throws Exception  {
		// TODO Auto-generated method stub
		
		//System.setProperty("hadoop.home.dir", "C:\\SparkDev");
		//System.setProperty("spark.sql.warehouse.dir", "C:\\spark-warehouse");
		//SparkConf conf = (new SparkConf()).setMaster("local").setAppName("HERS");
		
		SparkConf conf = new SparkConf().setAppName("HERS");
		conf.set("org.apache.spark.serializer.KryoSerializer",
		 "spark_serializer");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		String filePath_calCert = Property.getProperty("clean_data");
		String filePath_cheers = Property.getProperty("clean_data_cheers");
		String resultPath_calCERT = Property.getProperty("resultPath_calCERT");;
		String resultPath_cheers = Property.getProperty("resultPath_cheers");
		String referenceFilePath = Property.getProperty("referenceFilePath");
		String temp_folder = Property.getProperty("temp_folder");
		
		 /*
		 String filePath_calCert = "C:\\CEC_Documents\\HERS\\Curated\\calCERT\\Clean\\temp\\Curated_clean.csv";
		 String filePath_cheers = "C:\\CEC_Documents\\HERS\\Curated\\cheers\\Clean\\temp\\Curated_clean.csv";
		 String resultPath_calCERT = "C:\\CEC_Documents\\HERS\\Curated\\calCERT\\Clean\\curated_calCERT.csv";
		 String resultPath_cheers = "C:\\CEC_Documents\\HERS\\Curated\\cheers\\Clean\\curated_cheers.csv";
		 String referenceFilePath = "C:\\CEC_Documents\\HERS\\Reference.csv";
		 String temp_folder = "C:\\CEC_Documents\\HERS\\Curated\\temp";
		 */
		 DataFrame df_calCERT = Resources.createDF(filePath_calCert, "\t", sqlContext)
				 				 .withColumnRenamed("A07-Refrigerant type", "Refrigerant_type");
				 				//.withColumnRenamed("F05-Liquid Line Pressure", "F05_Liquid_Line_Pressure");
		 DataFrame df_cheers = Resources.createDF(filePath_cheers, "\t", sqlContext);
				 
		 DataFrame refernceFile = Resources.createDF(referenceFilePath, ",", sqlContext);
		 
		 DataFrame R410A_cheers = df_cheers.filter("A07 = 'R410A'").select("Registration_Number", "F05","G02");
		 DataFrame R22_cheers = df_cheers.filter("A07 = 'R22'").select("Registration_Number", "F05","G02");
		 
		 DataFrame R410A_calCERT = df_calCERT.filter("Refrigerant_type = 'R410A'").select("Registration_Number", "F05-Liquid Line Pressure","G02-Suction Line Pressure");
		 DataFrame R22_calCERT = df_calCERT.filter("Refrigerant_type = 'R22'").select("Registration_Number", "F05-Liquid Line Pressure","G02-Suction Line Pressure");
		 
		 DataFrame reference_temp = refernceFile.withColumnRenamed("Temp[F]", "Temp").select("Pressure_Liquid_psig","Temp");
		 
		 
		 
		 TreeSet <Double> pressure = Resources.toTreeSet(refernceFile.select("Pressure_Liquid_psig"));
		 ArrayList<String> curated_Data_R410A_calCERT = Resources.toArrayList(R410A_calCERT);
		 ArrayList<String> curated_Data_R22_calCERT = Resources.toArrayList(R22_calCERT);
		 
		 ArrayList<String> curated_Data_R410A_cheers = Resources.toArrayList(R410A_cheers);
		 ArrayList<String> curated_Data_R22_cheers = Resources.toArrayList(R22_cheers);
		
		 //reference_temp.registerTempTable("raw");
		 //DataFrame tempDF = sqlContext.sql("select Temp from raw where Pressure_Liquid_psig = '-14.21' OR Pressure_Liquid_psig = '-14.18'");
			//
		 //System.out.println(curated_Data);  
		 
		 //R22.show();
		 
		 ArrayList<String> calculatedTemp_R410A_cheers = Resources.calculateTempR410A(reference_temp, curated_Data_R410A_cheers, pressure, sqlContext);
		 DataFrame finaldf_R410A_cheers = Resources.createDFfromList(calculatedTemp_R410A_cheers, sqlContext);
		 
		 
		 ArrayList<String> calculatedTemp_R22_cheers = Resources.calculateTempR22(reference_temp, curated_Data_R22_cheers, pressure, sqlContext);
		 DataFrame finaldf_R22_cheers = Resources.createDFfromList(calculatedTemp_R22_cheers, sqlContext);
		 
		 	 
		 ArrayList<String> calculatedTemp_R410A_calCERT = Resources.calculateTempR410A(reference_temp, curated_Data_R410A_calCERT, pressure, sqlContext);
		 DataFrame finaldf_R410A_calCERT = Resources.createDFfromList(calculatedTemp_R410A_calCERT, sqlContext);
		 
		 ArrayList<String> calculatedTemp_R22_calCERT = Resources.calculateTempR22(reference_temp, curated_Data_R22_calCERT, pressure, sqlContext);
		 DataFrame finaldf_R22_calCERT = Resources.createDFfromList(calculatedTemp_R22_calCERT, sqlContext);
		 
		 		 
		 DataFrame final_calCERT_temp = finaldf_R410A_calCERT.unionAll(finaldf_R22_calCERT);
		 DataFrame final_cheers_temp = finaldf_R410A_cheers.unionAll(finaldf_R22_cheers);
		 
		 DataFrame final_calCERT= df_calCERT.join(final_calCERT_temp, df_calCERT.col("Registration_Number").equalTo(final_calCERT_temp.col("Registration_Number_temp")).and(df_calCERT.col("F05-Liquid Line Pressure").equalTo(final_calCERT_temp.col("F05_Liquid_Line_Pressure")))).drop("F05_Liquid_Line_Pressure").drop("Registration_Number_temp").drop("G02_Suction_Line_Pressure").withColumn("Data_Source", functions.lit("calCERT"));
		 DataFrame final_cheers= df_cheers.join(final_cheers_temp, df_cheers.col("Registration_Number").equalTo(final_cheers_temp.col("Registration_Number_temp")).and(df_cheers.col("F05").equalTo(final_cheers_temp.col("F05_Liquid_Line_Pressure")))).drop("F05_Liquid_Line_Pressure").drop("Registration_Number_temp").drop("G02_Suction_Line_Pressure").withColumn("Data_Source", functions.lit("CHEERS"));
		 
		 //df.withColumn("Cal_Temp", Resources.toTreeSet(refernceFile.select("Pressure Liquid [psig]")));
		 //System.out.println(pressure);
		 //final_cheers.show();
		 
		//Writing the files in the mentioned path and renaming the file in csv format
		 FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
		 fs.delete(new Path(temp_folder), true);
		 final_calCERT.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", "\t")
					.option("header", "true").save(temp_folder);
		 fs.rename(new Path(temp_folder+"/part-00000"),
					new Path(resultPath_calCERT));
		 fs.delete(new Path(temp_folder), true);
		 
		 fs.delete(new Path(temp_folder), true);
		 final_cheers.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", "\t")
					.option("header", "true").save(temp_folder);
		 fs.rename(new Path(temp_folder+"/part-00000"),
					new Path(resultPath_cheers));
		 
		 fs.delete(new Path(Property.getProperty("calcert_clean_finalprocess")), true);
		 fs.delete(new Path(Property.getProperty("cheers_clean_finalprocess")), true);
		 /*
		 fs.delete(new Path("C:\\CEC_Documents\\HERS\\Curated\\calCERT\\Clean\\temp"), true);
		 fs.delete(new Path("C:\\CEC_Documents\\HERS\\Curated\\cheers\\Clean\\temp"), true);
		 */ 
		  
	}


}

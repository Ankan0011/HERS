package com.hers.rawstage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import com.hers.utility.Property;

public class curatedToPurposed {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//System.setProperty("hadoop.home.dir", "C:\\SparkDev");
		//System.setProperty("spark.sql.warehouse.dir", "C:\\spark-warehouse");
		//SparkConf conf = (new SparkConf()).setMaster("local").setAppName("HERS");
		
		SparkConf conf = new SparkConf().setAppName("HERS");
		 conf.set("org.apache.spark.serializer.KryoSerializer",
		 "spark_serializer");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
		
		String sourcePath_calCERT = Property.getProperty("resultPath_calCERT");
		String sourcePath_cheers = Property.getProperty("resultPath_cheers");
		String resultFilePath = Property.getProperty("result_Path_Purposed");
		String temp_folder = Property.getProperty("temp_folder_Purposed");
		
		/*
		String sourcePath_calCERT = "C:\\CEC_Documents\\HERS\\Curated\\calCERT\\Clean\\curated_calCERT.csv";
		String sourcePath_cheers = "C:\\CEC_Documents\\HERS\\Curated\\cheers\\Clean\\curated_cheers.csv";
		String resultFilePath = "C:\\CEC_Documents\\HERS\\Purpose\\Purpose.csv";
		String temp_folder = "C:\\CEC_Documents\\HERS\\Purpose\\temp";
		*/
		//String[] columnSequence = {"Registration_Number","EnforcementAgency","City","ZipCode","InstallerCompany","RaterCompany","AuthorCompany","A07","F05","F06","G02","G03","Condenser_Desired_Temperature","Evaporator_Desired_Temperature","Data_Source"};
		
		DataFrame df_calCERT = Resources.createDF(sourcePath_calCERT, "\t", sqlContext);
		DataFrame df_cheers = Resources.createDF(sourcePath_cheers, "\t", sqlContext).select("Registration_Number","EnforcementAgency","City","ZipCode","InstallerCompany","RaterCompany","AuthorCompany","A07","F05","F06","G02","G03","Condenser_Desired_Temperature","Evaporator_Desired_Temperature","Data_Source");
		
		DataFrame purposedDF = df_calCERT.unionAll(df_cheers);
		//purposedDF.show();
		FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
		 fs.delete(new Path(temp_folder), true);
		 purposedDF.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", "\t")
					.option("header", "true").save(temp_folder);
		 fs.rename(new Path(temp_folder+"/part-00000"),
					new Path(resultFilePath));
		 fs.delete(new Path(temp_folder), true);

	}

}

package com.hers.rawstage;

import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class Resources {
	public static DataFrame createDF(String filePath, String delimiter, SQLContext sqlContext)
	{
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter",delimiter).option("mode", "permissive").load(filePath );
		return df;
		
	}
	
	public static TreeSet <Double> toTreeSet(DataFrame dataFrame){
		TreeSet <Double> list = new TreeSet <Double>();
		 Row[] rows = dataFrame.collect();
		 for (Row row : rows) {
			 list.add(Double.parseDouble( row.toString().substring(1, row.toString().length()-1)));
			}
		return list;
		
	}
	
	public static ArrayList<String> toArrayList(DataFrame dataFrame){
		ArrayList<String> list = new ArrayList<String>();
		 Row[] rows = dataFrame.collect();
		 for (Row row : rows) {
			 list.add(row.toString().substring(1, row.toString().length()-1));
			}
		return list;
		
	}
	
	public static DataFrame createDFfromList(ArrayList<String> rawMissingList, SQLContext sqlContext)
	{
		DataFrame rawMissingdf = sqlContext.createDataset(rawMissingList, Encoders.STRING()).toDF();
		rawMissingdf = rawMissingdf.selectExpr("split(value, ',')[0] as Registration_Number_temp", 
				 "split(value, ',')[1] as F05_Liquid_Line_Pressure", 
				 "split(value, ',')[2] as G02_Suction_Line_Pressure", 
				 "split(value, ',')[3] as Condenser_Desired_Temperature",
				 "split(value, ',')[4] as Evaporator_Desired_Temperature" );
		return rawMissingdf;
		
	}
	
	public static ArrayList<String> calculateTempR410A(DataFrame dataFrame, ArrayList<String> mainDF ,TreeSet <Double> pressure , SQLContext sqlContext)
	{
		
		DataFrame tempDF = null;
		DataFrame tempDF02 = null;
		Double sum_temp = 0.0;
		Double sum_temp_02 = 0.0;
		Double avg_temp = 0.0;
		Double avg_temp_02 = 0.0;
		String finalval = null;
		String finalval02 = null;
		ArrayList<String> final_list = new ArrayList<String>();
		String TempRawtable = "rawtable";
		dataFrame.registerTempTable(TempRawtable);
		
		for(int i=0 ; i <= mainDF.size()-1; i++){
			String pres_psig;
			String pres_psig_02;
			String floor_val;
			String floor_val_02;
			String ceiling_val;
			String ceiling_val_02;
			sum_temp = 0.0;
			sum_temp_02 = 0.0;
			avg_temp = 0.0;
			avg_temp_02 = 0.0;
			finalval = "";
			finalval02 = "";
			ArrayList<String> temp_list = new ArrayList<String>();
			ArrayList<String> temp_list_02 = new ArrayList<String>();
			
			try{
				pres_psig = mainDF.get(i).split(",")[2];
				}catch (Exception e){ pres_psig = ""; }
			try{
				pres_psig_02 = mainDF.get(i).split(",")[1];
				}catch(Exception e){ pres_psig_02 = ""; }
				
			if(pres_psig.length() != 0 ){
				floor_val = pressure.floor(Double.parseDouble(pres_psig)-14.7).toString();
				ceiling_val = pressure.ceiling(Double.parseDouble(pres_psig)-14.7).toString();
				if(floor_val != null && ceiling_val != null){
					tempDF = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val+"' OR Pressure_Liquid_psig = '"+ceiling_val+"'");
					//averageList = Resources.toArrayList(tempDF);
				}
				else if(floor_val != null && ceiling_val == null){
					tempDF = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val+"'");
					}
				else if(floor_val == null && ceiling_val != null){
					tempDF = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+ceiling_val+"'");
					}
				temp_list = Resources.toArrayList(tempDF);
				for(String d : temp_list){	sum_temp += Double.parseDouble(d); }
				avg_temp = (double) (sum_temp / temp_list.size()) ;
				finalval = avg_temp.toString();
			}
			else{
				finalval = "";
			}
			if(pres_psig_02.length() != 0 ){
			floor_val_02 = pressure.floor(Double.parseDouble(pres_psig_02)-14.7).toString();
			ceiling_val_02 = pressure.ceiling(Double.parseDouble(pres_psig_02)-14.7).toString();
			if(floor_val_02 != null && ceiling_val_02 != null){
				tempDF02 = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val_02+"' OR Pressure_Liquid_psig = '"+ceiling_val_02+"'");
				//averageList = Resources.toArrayList(tempDF);
			}
			else if(floor_val_02 != null && ceiling_val_02 == null){
				tempDF02 = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val_02+"'");
				}
			else if(floor_val_02 == null && ceiling_val_02 != null){
				tempDF02 = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+ceiling_val_02+"'");
				}
			temp_list_02 = Resources.toArrayList(tempDF02);
			for(String e : temp_list_02){	sum_temp_02 += Double.parseDouble(e); }
			avg_temp_02 = (double) (sum_temp_02 / temp_list_02.size()) ;
			finalval02 = avg_temp_02.toString();
			}
			else{
				finalval02 = "";
			}
			final_list.add(mainDF.get(i)+","+finalval02+","+finalval) ;
		}
		
		
		return final_list;
		
	}
	public static ArrayList<String> calculateTempR22(DataFrame dataFrame, ArrayList<String> mainDF ,TreeSet <Double> pressure , SQLContext sqlContext)
	{
		
		DataFrame tempDF = null;
		DataFrame tempDF02 = null;
		Double sum_temp = 0.0;
		Double sum_temp_02 = 0.0;
		Double avg_temp = 0.0;
		Double avg_temp_02 = 0.0;
		String finalval = null;
		String finalval02 = null;
		ArrayList<String> final_list = new ArrayList<String>();
		String TempRawtable = "rawtable";
		dataFrame.registerTempTable(TempRawtable);
		
		for(int i=0 ; i <= mainDF.size()-1; i++){
			String pres_psig;
			String pres_psig_02;
			String floor_val;
			String floor_val_02;
			String ceiling_val;
			String ceiling_val_02;
			sum_temp = 0.0;
			sum_temp_02 = 0.0;
			avg_temp = 0.0;
			avg_temp_02 = 0.0;
			finalval = "";
			finalval02 = "";
			ArrayList<String> temp_list = new ArrayList<String>();
			ArrayList<String> temp_list_02 = new ArrayList<String>();
			try{
			pres_psig = mainDF.get(i).split(",")[2];
			}catch (Exception e){ pres_psig = ""; }
			try{
			pres_psig_02 = mainDF.get(i).split(",")[1];
			}catch(Exception e){ pres_psig_02 = ""; }
			
			if(pres_psig.length() != 0 ){
				floor_val = pressure.floor(Double.parseDouble(pres_psig)).toString();
				ceiling_val = pressure.ceiling(Double.parseDouble(pres_psig)).toString();
				if(floor_val != null && ceiling_val != null){
					tempDF = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val+"' OR Pressure_Liquid_psig = '"+ceiling_val+"'");
					//averageList = Resources.toArrayList(tempDF);
				}
				else if(floor_val != null && ceiling_val == null){
					tempDF = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val+"'");
					}
				else if(floor_val == null && ceiling_val != null){
					tempDF = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+ceiling_val+"'");
					}
				temp_list = Resources.toArrayList(tempDF);
				for(String d : temp_list){	sum_temp += Double.parseDouble(d); }
				avg_temp = (double) (sum_temp / temp_list.size()) ;
				finalval = avg_temp.toString();
			}
			else{
				finalval = "";
			}
			if(pres_psig_02.length() != 0 ){
			floor_val_02 = pressure.floor(Double.parseDouble(pres_psig_02)).toString();
			ceiling_val_02 = pressure.ceiling(Double.parseDouble(pres_psig_02)).toString();
			if(floor_val_02 != null && ceiling_val_02 != null){
				tempDF02 = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val_02+"' OR Pressure_Liquid_psig = '"+ceiling_val_02+"'");
				//averageList = Resources.toArrayList(tempDF);
			}
			else if(floor_val_02 != null && ceiling_val_02 == null){
				tempDF02 = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+floor_val_02+"'");
				}
			else if(floor_val_02 == null && ceiling_val_02 != null){
				tempDF02 = sqlContext.sql("select Temp from rawtable where Pressure_Liquid_psig = '"+ceiling_val_02+"'");
				}
			temp_list_02 = Resources.toArrayList(tempDF02);
			for(String e : temp_list_02){	sum_temp_02 += Double.parseDouble(e); }
			avg_temp_02 = (double) (sum_temp_02 / temp_list_02.size()) ;
			finalval02 = avg_temp_02.toString();
			}
			else{
				finalval02 = "";
			}
			final_list.add(mainDF.get(i)+","+finalval02+","+finalval) ;
		}
		
		
		return final_list;
	}
	
}

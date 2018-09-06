package com.hers.rawstage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class CreateDF {
  def CreateDF(File_Path: String, delimiter: String, sqlContext: SQLContext): DataFrame =
    {
      /**
       * This method will give the current timestamp
       * input:- Unit
       * output:- Timestamp:String
       */
      val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", delimiter).load(File_Path)
    
      return df
    }
}
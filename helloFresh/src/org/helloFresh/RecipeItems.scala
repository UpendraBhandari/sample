package org.helloFresh

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import  org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.orc._

// Date Functions
import java.util.Date
import java.text.SimpleDateFormat



object RecipeItems {
  
  def main(args: Array[String]) = {
    
     // Initialization of Spark and HiveContext
     val sparkConf = new SparkConf().setAppName("helloFresh - RecipeList")
     val sc = new SparkContext(sparkConf)
     val sqlContext = new SQLContext(sc)
     import sqlContext.implicits._
     
     // read the files passed by user as the argument   
     val recipeItems = sqlContext.read.format("json").load("file:///home/cloudera/ETLJars/recipeitems-latest.json")
     recipeItems.registerTempTable("recipeItems")
             
     /*
      *  Filter recipe list which contain beef. 
      *  Convert prepTime and cookTime into hours. Minute time is divided by 60 to get hours rounding of the result with precision of 2
      */
     /*
      val recipeTime = sqlContext.sql("""
      select 
      *,
      case 
       --if prepTime column has Minutes entry
       when (substr(nvl(prepTime,'PT00M'),-1)=='M') then
        case 
          --if the prepTime column has Hours and Minutes entry
          when (prepTime like '%H%') then 
           regexp_replace(substr(prepTime,3,2),'[^0-9]','')+ round(regexp_replace(substr(prepTime,-3), '[^0-9]','')/60,2)
          else
          --else prepTime colum has Minutes entry only
           round(regexp_replace(prepTime, '[^0-9]','')/60,2)
        end 
        --if prepTime column has only Hours entry
        when (substr(nvl(prepTime,'PT00H'),-1)=='H') then
         regexp_replace(prepTime, '[^0-9]','')
      end as prepTimeinHrs,
      
      case 
       --if cookTime column has Minutes entry
       when (substr(nvl(cookTime,'PT00M'),-1)=='M') then
       case 
       --if the cookTime column has Hours and Minutes entry
         when (cookTime like '%H%') then 
          regexp_replace(substr(cookTime,3,2),'[^0-9]','')+ round(regexp_replace(substr(cookTime,-3), '[^0-9]','')/60,2)
         else
         --else cookTime column has Minutes entry only
          round(regexp_replace(cookTime, '[^0-9]','')/60,2)
       end 
       --if cookTime column has only Hours entry
       when (substr(nvl(cookTime,'PT00H'),-1)=='H') then
         regexp_replace(cookTime, '[^0-9]','')  
      end as cookTimeinHrs      
      from 
      recipeItems where lower(ingredients) like '%beef%'   
      """)
     */
     
     
     val recipeTime = sqlContext.sql("""
      select 
      *,
      case            
      when (prepTime like '%D%') then
       case         
         when (substr(nvl(prepTime,'PT00M'),-1)='M') then
          case             
            when (prepTime like '%H%') then
             (regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24) + 
             regexp_replace(substr(prepTime,4,2),'[^0-9]','')+ 
             round(regexp_replace(substr(prepTime,-3), '[^0-9]','')/60,2)
            else            
              regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24 + 
              round(regexp_replace(substr(prepTime,-3), '[^0-9]','')/60,2)
           end         
         when (substr(nvl(prepTime,'PT00H'),-1)='H') then
          regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24 + 
          regexp_replace(substr(prepTime,-3),'[^0-9]','')
         else         
         regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24
       end             
       when (substr(nvl(prepTime,'PT00M'),-1)='M' and prepTime not like '%D%') then
        case           
          when (prepTime like '%H%') then 
           regexp_replace(substr(prepTime,3,2),'[^0-9]','')+ 
           round(regexp_replace(substr(prepTime,-3), '[^0-9]','')/60,2)
          else          
           round(regexp_replace(prepTime, '[^0-9]','')/60,2)
        end         
        when (substr(nvl(prepTime,'PT00H'),-1)='H') then
         regexp_replace(prepTime, '[^0-9]','')
      end as prepTimeinHrs,      
      
      case 
      -- For cookTime, check whether it has Day entry      
      when (cookTime like '%D%') then
       case 
         -- If "Yes", Check whether it has Minutes entry
         when (substr(nvl(cookTime,'PT00M'),-1)='M') then
          case 
            --if the cookTime column has Day, Hours and Minutes entry
            when (cookTime like '%H%') then
             (regexp_replace(substr(cookTime,2,1),'[^0-9]','')*24) + 
             regexp_replace(substr(cookTime,4,2),'[^0-9]','')+ 
             round(regexp_replace(substr(cookTime,-3), '[^0-9]','')/60,2)
            else
            --else cookTime column has Day and Minutes entry only
              regexp_replace(substr(cookTime,2,1),'[^0-9]','')*24 + 
              round(regexp_replace(substr(cookTime,-3), '[^0-9]','')/60,2)
           end
         --if cookTime column has Day and Hours entry only
         when (substr(nvl(cookTime,'PT00H'),-1)='H') then
          regexp_replace(substr(cookTime,2,1),'[^0-9]','')*24 + 
          regexp_replace(substr(cookTime,-3),'[^0-9]','')
         --if entry has only days column
        else 
         -- cookTime column has Day  entry only
         regexp_replace(substr(cookTime,2,1),'[^0-9]','')*24
        end          
      
       --if cookTime column has Minutes entry and no Day entry
       when (substr(nvl(cookTime,'PT00M'),-1)='M' and cookTime not like '%D%') then
       case 
       --if the cookTime column has Hours and Minutes entry
         when (cookTime like '%H%') then 
          regexp_replace(substr(cookTime,3,2),'[^0-9]','')+ 
          round(regexp_replace(substr(cookTime,-3), '[^0-9]','')/60,2)
         else
         --else cookTime column has Minutes entry only
          round(regexp_replace(cookTime, '[^0-9]','')/60,2)
       end 
       --if cookTime column has only Hours entry
       when (substr(nvl(cookTime,'PT00H'),-1)='H') then
         regexp_replace(cookTime, '[^0-9]','')  
      end as cookTimeinHrs      
      from 
      recipeItems where lower(ingredients) like '%beef%'   
      """)
     
     recipeTime.registerTempTable("temprecipeTable")
     
     val finalResult = sqlContext.sql("""
       
       select *, 
       case
       when (prepTimeinHrs+cookTimeinHrs > 1.00) then
       "Hard"
       when (prepTimeinHrs+cookTimeinHrs between 0.50 and 1.00) then
       "Medium"
       when (prepTimeinHrs+cookTimeinHrs < 0.50) then
       "Easy"
       else
       "Unknown"
       end as difficulty
       from temprecipeTable
            
       """)
       
     finalResult.registerTempTable("finaltable")
    
     //sqlContext.sql("select prepTime,cookTime,prepTimeinHrs,cookTimeinHrs,difficulty from finaltable").foreach(println)
     
     // remove extra column created for processing and create dataframe with original table plus the difficulty column only
     val output = sqlContext.sql("select `"+recipeItems.columns(0)+"`,"+recipeItems.columns.drop(1).mkString(",")+" ,difficulty from finaltable")
     output.write.mode("overwrite").parquet("/user/cloudera/beefRecipe")    
    
  }
  
  
}
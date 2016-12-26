'''
Created on 26-Dec-2016

@author: upendra
'''

import sys
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SparkContext, SparkConf



def RecipeItems(sc,inputfile,outputlocation):
    # read the files passed by user as the argument 
    sqlContext = SQLContext(sc)
    recipeItems = sqlContext.read.format("json").load("file:///home/cloudera/ETLJars/recipeitems-latest.json")
    recipeItems.registerTempTable("recipeItems")
   
    
    # Business Logic to convert prepTime & cookTime into Hours
    recipeTime = sqlContext.sql("""
      select 
      *,
      case 
      -- For prepTime, check whether it has Day entry      
      when (prepTime like '%D%') then
       case 
        -- If "Yes", Check whether it has Minutes entry
         when (substr(nvl(prepTime,'PT00M'),-1)='M') then
          case 
            --Check if the prepTime column has Day, Hours and Minutes entry
            when (prepTime like '%H%') then
             (regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24) + 
             regexp_replace(substr(prepTime,4,2),'[^0-9]','')+ 
             round(regexp_replace(substr(prepTime,-3), '[^0-9]','')/60,2)
            else
            --else prepTime column has Day and Minutes entry only
              regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24 + 
              round(regexp_replace(substr(prepTime,-3), '[^0-9]','')/60,2)
           end
         --if prepTime column has only Day and Hours entry only
         when (substr(nvl(prepTime,'PT00H'),-1)='H') then
          regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24 + 
          regexp_replace(substr(prepTime,-3),'[^0-9]','')
         
        else 
        --it has only Day entry
         regexp_replace(substr(prepTime,2,1),'[^0-9]','')*24
       end                     
       
       --if prepTime column has Minutes entry but no Day entry
       when (substr(nvl(prepTime,'PT00M'),-1)='M' and prepTime not like '%D%') then
        case 
          --if the prepTime column has Hours and Minutes entry
          when (prepTime like '%H%') then 
           regexp_replace(substr(prepTime,3,2),'[^0-9]','')+ 
           round(regexp_replace(substr(prepTime,-3), '[^0-9]','')/60,2)
          else
          --else prepTime colum has Minutes entry only
           round(regexp_replace(prepTime, '[^0-9]','')/60,2)
        end 
        --if prepTime column has only Hours entry
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
    
    # use the business logic provided to mark recipes as Hard, Medium, Easy, Unknown
    finalResult = sqlContext.sql("""       
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
    
    # Create final table with columns needed only. existing column from source and difficulty column.    
    firstCol = recipeItems.columns.pop(0) 
    restCols = ','.join(recipeItems.columns[1:])
    output = sqlContext.sql("select `"+firstCol+"`,"+restCols+",difficulty from finaltable")
    
    # Store output into directory in HDFS as parquet
    output.write.mode("overwrite").parquet("hdfs:///user/cloudera/beefRecipe") 
    
    
    pass


if __name__ == '__main__':    
    if (len(sys.argv)==3):
        conf = SparkConf().setAppName("HelloFresh - Recipe Test v0.1")
        sc = SparkContext(conf=conf)
        RecipeItems(sc,sys.argv[1],sys.argv[2])
        sc.stop()
    else:
        print "Usage: spark-submit RecipeMain.py <full path to input file> <full path to ouputt location in hdfs>\n"
        print "example: spark-submit RecipeMain.py file:///home/cloudera/ETLJars/recipeitems-latest.json hdfs:///user/cloudera/beefRecipe"
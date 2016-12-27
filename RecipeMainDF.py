'''
Created on 27-Dec-2016

@author: uppi
'''

import sys
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import udf
import re


'''
Algorithm takes prepTime and cookTime columns and parse them.
The column value is converted into hours
Assumption is made that the order of charactes in value will be 'D', 'H', 'M'
If order of characters in prepTime and cookTime is changed, output will be invalid

'''

def calcDay(value):
    #print 'Inside Day'
    temp = value.split('D')
    day = int(''.join(re.findall(r'[0-9]',temp[0],0))) * 24.0
    rest = temp[1]
    valueinHrs= 0 
    
    
    if(len(rest)!=0):
        # if Hours entry in value
        if 'H' in rest:
            valueinHrs = calcHrs(rest)*1
        
        # if only Minutes entry
        if 'M' in rest and 'H' not in rest:
            valueinHrs = calcMin(rest)
    else:
        None
    
    return day + valueinHrs


def calcHrs(value):
    #print 'In Hours'
    temp = value.split('H')
    hrs = int(''.join(re.findall(r'[0-9]',temp[0],0)))
    rest = temp[1]
    valueinHrs= 0 
    
    if (len(rest)!=0):
        if 'M' in rest:
            valueinHrs = calcMin(rest)
    
    return hrs + valueinHrs    
    

def calcMin(value):
    #print 'In Minutes'
    temp = value.split('M')
    minutes = int(''.join(re.findall(r'[0-9]',temp[0],0)))/60.0
    
    return minutes


def parseTime(x,y):
    
    try:  
        cookTime = x
        prepTime = y
        
        #print prepTime,cookTime
        
        # initialize variables
        prepTimeinHrs = 0
        cookTimeinHrs = 0
        totalTime = 0
        
        # default difficulty
        difficulty = "Unknown"
        
        # parse prepTime
        if ('D' in prepTime):
            prepTimeinHrs = calcDay(prepTime)
            
        elif ('H' in prepTime and 'D' not in prepTime):
            prepTimeinHrs = calcHrs(prepTime)
        
        elif ('M' in prepTime and 'H' not in prepTime and 'D' not in prepTime):
            prepTimeinHrs = calcMin(prepTime)
        else:
            pass
            
        
        #parseCookTime
        if ('D' in cookTime):
            cookTimeinHrs = calcDay(cookTime)
            
        elif ('H' in cookTime and 'D' not in cookTime):
            cookTimeinHrs = calcHrs(cookTime)
        
        elif ('M' in cookTime and 'H' not in cookTime and 'D' not in cookTime):
            cookTimeinHrs = calcMin(cookTime)
        else:
            pass
            
        
        
        totalTime = prepTimeinHrs + cookTimeinHrs
        
        # business logic as mentioned in test
        if totalTime > 1.0:
            difficulty = "Hard"
        elif totalTime >= 0.5 and totalTime <= 1.0:
            difficulty = "Medium"
        elif totalTime < 0.5:
            difficulty = "Easy"
        else:
            difficulty = 'Unknown'
            
        #print totalTime, difficulty
    except:
        difficulty = "Unknown"
            
    return difficulty   
    
    
def RecipeItemsDF(sc,inputfile,outputlocation):
    
    sqlContext = SQLContext(sc)
    
    # read the input file provided by the user
    recipeItems = sqlContext.read.format("json").load(inputfile)
    recipeItems.registerTempTable("recipeItems")
    
    # register the UDF
    sqlContext.registerFunction("parseTime",parseTime,StringType())
    
    beefRecipe = sqlContext.sql("select *,parseTime(cookTime,prepTime) as difficulty from recipeItems where lower(ingredients) like '%beef%'")
   
    #store the final output
    beefRecipe.write.mode("overwrite").save(outputlocation+"/beefRecipeWithDifficulty.parquet", format="parquet")
    


if __name__ == '__main__':
    if (len(sys.argv)==3):
        conf = SparkConf().setAppName("HelloFresh - Recipe Test v0.1")
        sc = SparkContext(conf=conf)
        RecipeItemsDF(sc,sys.argv[1],sys.argv[2])
        sc.stop()
    else:
        print "Usage: spark-submit RecipeMain.py <full path to input file> <full path to ouputt location in hdfs>\n"
        print "example: spark-submit RecipeMain.py file:///home/cloudera/ETLJars/recipeitems-latest.json hdfs:///user/cloudera/beefRecipe"
    
        
    
# Databricks notebook source
# MAGIC %md
# MAGIC # Proyecto de primer parcial de infraestructura para la ciencia de datos
# MAGIC ## Ricardo López Rodríguez A01066515

# COMMAND ----------

# MAGIC %md ## Pandas User-defined Functions
# MAGIC 
# MAGIC Before starting, please make sure to read the following documents, and use them to solve any concerns:
# MAGIC   - https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html?_ga=2.107754946.1571903824.1587912461-1262232029.1567646027
# MAGIC   - https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html.
# MAGIC   
# MAGIC 
# MAGIC Generally speaking, we can say that a pandas UDF connects Spark to Pandas' universe by:
# MAGIC    1. Splitting an input Spark column or columns into small batches (pandas.Series or pandas.DataFrames).
# MAGIC    2. Applying a method coded in a Pandas environment for each batch as a subset of the data, *in parallel*.
# MAGIC    3. Concatenating the result of each batch and returning it into a Spark DataFrame.
# MAGIC  
# MAGIC It is like splitting a big DataFrame (Spark) into small datasets (Pandas) that are processed independently by diferrent machines, in parallel.
# MAGIC  
# MAGIC As you already read in the previous links, there are a number of user-defined functions that can be used depending on the type of operation you want to implement. Next is a brief description along to an example for each. We will be using the temperature database that looks like this:

# COMMAND ----------

# MAGIC %python
# MAGIC import pyspark.sql.functions as F
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC temperature_db = spark.read.format("csv").option("header",True).load("/FileStore/tables/day.csv")\
# MAGIC                       .select(F.col("dteday").cast(T.DateType()).alias("calday"),
# MAGIC                               F.col("temp").cast(T.DoubleType()))
# MAGIC 
# MAGIC display(
# MAGIC   temperature_db
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scalar UDFs
# MAGIC Operates on a single column with scalar operators (+, -, /, * , etc). The result of the UDF must be independent of the splitting strategy that Spark does internally. 
# MAGIC - Input: pandas.Series
# MAGIC - Output: pandas.Series of the same length as the input.

# COMMAND ----------

# MAGIC %python
# MAGIC # Scalar UDF example
# MAGIC from pyspark.sql.functions import col, pandas_udf, PandasUDFType
# MAGIC import numpy as np
# MAGIC import statistics as stat
# MAGIC 
# MAGIC # Declare the function
# MAGIC def multiply_func(a, b):
# MAGIC     # Returns the product of two columns
# MAGIC     return a * b
# MAGIC 
# MAGIC def double_func(a):
# MAGIC     # Returns the double of a column
# MAGIC     return a * 2.0
# MAGIC 
# MAGIC # Create the UDF by setting the function and the expected output schema
# MAGIC multiply = pandas_udf(multiply_func, returnType=T.DoubleType())
# MAGIC double = pandas_udf(double_func, returnType=T.DoubleType())
# MAGIC 
# MAGIC display(
# MAGIC   temperature_db.select(col("*"), # Selects all columns in dataframe
# MAGIC                         multiply(col("temp"), col("temp")),
# MAGIC                         double(col("temp")))
# MAGIC )

# COMMAND ----------

# MAGIC %md ##### Activity # 1
# MAGIC Using scalar UDFs, implement the function that returns the median temperature of the previous 4 days.  

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #median: Function that computes the median of the previous 4 records in a pandas serie
# MAGIC def median(pd_serie):
# MAGIC   #Computing the rolling median
# MAGIC   #shifting the result by one period
# MAGIC   return pd_serie.rolling(4).median().shift()
# MAGIC 
# MAGIC #Creation of a scalar UDF...
# MAGIC rolling_median = pandas_udf(median, returnType=T.DoubleType())
# MAGIC 
# MAGIC display(temperature_db.select(col("*"),rolling_median(col("temp"))))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Grouped map UDFs
# MAGIC Follows the “split-apply-combine” pattern by combining a UDF with groupBy().apply(). The process consists in three steps:
# MAGIC   1. Split the data into groups by using DataFrame.groupBy.
# MAGIC   2. Apply a function to each group generated. The input and output of the function are now both pandas.DataFrame.
# MAGIC   3. Combine the results into a new Spark DataFrame.
# MAGIC 
# MAGIC Just as the scalar UDFs, grouped map UDFs need to have the output schema predefined.
# MAGIC 
# MAGIC - Input: pandas.DataFrame
# MAGIC - Output: pandas.DataFrame that can have a different length.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import PandasUDFType
# MAGIC 
# MAGIC # Subtracts the weekly mean temperature to each day, per week. 
# MAGIC @pandas_udf("calday date, temp double, week string", PandasUDFType.GROUPED_MAP)
# MAGIC def subtract_mean(pandasdf):
# MAGIC     # pandasdf is a pandas.DataFrame
# MAGIC     temp_series = pandasdf.temp # get the temp column as a pandas.Series
# MAGIC     return pandasdf.assign(temp=temp_series - temp_series.mean()) # Change the value of temp, given the weekly mean
# MAGIC 
# MAGIC # Keeps the cumulative max temperature per week.
# MAGIC @pandas_udf("calday date, temp double, week string", PandasUDFType.GROUPED_MAP)
# MAGIC def cummax(pandasdf):
# MAGIC     # pandasdf is a pandas.DataFrame
# MAGIC     cummax_temp_series = pandasdf.temp.cummax() # get the temp column as a pandas.Series
# MAGIC     return pandasdf.assign(temp=cummax_temp_series) # Change the value of temp to the precomputed cummax
# MAGIC 
# MAGIC 
# MAGIC # Just adding "week" column
# MAGIC temperature_db_with_week = temperature_db.withColumn("week", F.concat(F.year(col("calday")), F.weekofyear(col("calday"))))
# MAGIC 
# MAGIC # Computing cummax
# MAGIC cummax_temp = temperature_db_with_week.groupby("week").apply(cummax)\
# MAGIC                                       .withColumnRenamed("temp", "cummax_temp")\
# MAGIC                                       .drop("week")
# MAGIC 
# MAGIC # Computing the mean subtraction
# MAGIC subtract_mean_temp = temperature_db_with_week.groupby("week").apply(subtract_mean)\
# MAGIC                                              .withColumnRenamed("temp", "mean_subtracted_temp")\
# MAGIC                                              .drop("week")
# MAGIC 
# MAGIC display(
# MAGIC   cummax_temp.join(subtract_mean_temp, ["calday"], "inner")
# MAGIC              .join(temperature_db_with_week, ["calday"], "inner") # To recover "week" and "temp" columns
# MAGIC              .select("calday", "week", "temp", "mean_subtracted_temp", "cummax_temp")
# MAGIC )

# COMMAND ----------

# MAGIC %md #### Activity #2
# MAGIC 
# MAGIC Implement a Grouped map UDF that ranks the temperature records of each week in ascending order (lowest temp should have the lowest rank). Define and explain how are you handling ties.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #Limpiando un poco el dataSet ...
# MAGIC cleaned_data_set = temperature_db_with_week.filter((col("calday") >"2011-01-02") & (col("calday") < "2021-12-31"))
# MAGIC 
# MAGIC 
# MAGIC #Function that ranks temperature recrds of each week in ascending order 
# MAGIC @pandas_udf("calday date, temp double, week string, temp_ranking double", PandasUDFType.GROUPED_MAP)
# MAGIC def temperature_ranking(df):
# MAGIC     #
# MAGIC     #Ranking of temperature values
# MAGIC     temp_serie = df.temp.rank(method = "max")    
# MAGIC     return df.assign(temp_ranking = temp_serie)
# MAGIC   
# MAGIC   
# MAGIC 
# MAGIC #Aplying temperature ranking and ordering by ascending calday .... 
# MAGIC display(
# MAGIC   cleaned_data_set.groupBy("week").apply(temperature_ranking).orderBy(F.asc(col("calday")))
# MAGIC )
# MAGIC 
# MAGIC ####################
# MAGIC #HANDLING TIES :
# MAGIC # We handled ties using the method = "max" in rank() pandas function. This 
# MAGIC # mehod selects the highest rank in the group, i.e. the records that have the
# MAGIC #the same value are ranked using the highest rank. 
# MAGIC ###################
# MAGIC #RANKING MEANING:
# MAGIC # Our ranking enumarates temperatures of each week from
# MAGIC # 1 to 7 where the former is the lowest temperature and the 
# MAGIC # latter represents the highest temperature. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Case Study
# MAGIC Note that the notebook is written with Python as the default language. This is because the most common use that one can give to UDFs is to apply python libraries/methods/models to spark DataFrames. In the next cells you will see how a linear regression can be fitted using pandas. Next, you will fit a LinearRegression model from scikit-learn with a spark column, save the coefficients and apply them to another spark DataFrame.

# COMMAND ----------

# MAGIC %md #### Pandas implementation of LinearRegression
# MAGIC The next example takes the temperature database and fits a linear regression using pure python.

# COMMAND ----------

# MAGIC %python
# MAGIC import pyspark.sql.functions as F
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC import pandas as pd
# MAGIC 
# MAGIC # Read the file that we saved in the distributed file system (DBFS)e
# MAGIC temperature_db = spark.read.format("csv").option("header", True).load("/FileStore/tables/day.csv")\
# MAGIC                       .select(F.col("dteday").cast(T.DateType()).alias("calday"),
# MAGIC                               F.col("temp").cast(T.DoubleType()),
# MAGIC                               F.col("hum").cast(T.DoubleType()),
# MAGIC                               F.col("windspeed").cast(T.DoubleType()))
# MAGIC 
# MAGIC temperature_db_python = temperature_db.toPandas() # Converting into pandas dataframe
# MAGIC 
# MAGIC display(temperature_db_python)

# COMMAND ----------

# MAGIC %md We will take the columns "hum" and "windspeed" of whole dataset to train the model to predict the temperature in column "temp". The trained model will be called "lr".

# COMMAND ----------

# MAGIC %python
# MAGIC from sklearn.linear_model import LinearRegression
# MAGIC 
# MAGIC X = temperature_db_python[["hum", "windspeed"]] # train variables
# MAGIC y = temperature_db_python[["temp"]] # target
# MAGIC 
# MAGIC lr = LinearRegression(fit_intercept=True, normalize=False).fit(X, y) # Fit model
# MAGIC 
# MAGIC # We can extract some information from the "lr" object trained before:
# MAGIC print(lr.coef_) # Coefficients
# MAGIC print(lr.intercept_) # Intercept

# COMMAND ----------

# MAGIC %md #### Interpretation
# MAGIC Using the extracted information, we can build the linear regression equation as follows:
# MAGIC 
# MAGIC \\(temp = (0.12015) \\times hum + (- 0.31819) \\times windspeed + (0.48055)\\)
# MAGIC 
# MAGIC Note that variable order is the same as the one in the X variable defined above. By the coefficients' sign we can see that as the humidity rises, also does the temperature. However, as the windspeed increases the temperature decreases.

# COMMAND ----------

# MAGIC %md #### Evaluating on new data
# MAGIC To get the expected temperature given a humidity of 0.75 and a windspeed of 0.28 we just need to evaluate the function as:
# MAGIC 
# MAGIC \\(temp = 0.12015\\times 0.75 - 0.31819\\times 0.28 + 0.48055\\)
# MAGIC 
# MAGIC \\(temp = 0.4815693\\)

# COMMAND ----------

# MAGIC %md #### Spark implementation with UDFs
# MAGIC 
# MAGIC The goal is to encapsulate the training and extraction of the coefficients and intercept into a UDF so it can be applied in parallel. We will split the dataset into train and test, where the train set is the first year of observations. The train split is used to get the coefficients of the linear regression, which are then applied to the test set so we can get the predicted temperature.

# COMMAND ----------

# MAGIC %python
# MAGIC import pyspark.sql.functions as F
# MAGIC import pyspark.sql.types as T
# MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
# MAGIC 
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from sklearn.linear_model import LinearRegression
# MAGIC 
# MAGIC # Read database
# MAGIC temperature_db = spark.read.format("csv").option("header",True).load("/FileStore/tables/day.csv")\
# MAGIC                       .select(F.col("dteday").cast(T.DateType()).alias("calday"),
# MAGIC                               F.col("temp").cast(T.DoubleType()),
# MAGIC                               F.col("hum").cast(T.DoubleType()),
# MAGIC                               F.col("windspeed").cast(T.DoubleType()))
# MAGIC 
# MAGIC # Split the dataset into train and test given the day
# MAGIC train = temperature_db.filter(F.col("calday")<"2012-01-01")
# MAGIC test = temperature_db.filter(F.col("calday")>="2012-01-01")
# MAGIC 
# MAGIC #Function that computes a linear equation from a pandas dataFrame, and returs coeficients and
# MAGIC # intercept in a dataFrame
# MAGIC @pandas_udf("hum_coeff double, windspeed_coeff double, intercept double", PandasUDFType.GROUPED_MAP)
# MAGIC def linearRegressionFunction(pdf):
# MAGIC   X = pdf[["hum","windspeed"]] #predictors
# MAGIC   Y = pdf[["temp"]] #predicted variable
# MAGIC   model = LinearRegression(fit_intercept=True, normalize=False).fit(X,Y) #Fit the model
# MAGIC 
# MAGIC   #Extraction of coefficients ...
# MAGIC   results = dict(hum_coeff =model.coef_[0][0], windspeed_coeff = model.coef_[0][1],
# MAGIC                  intercept = model.intercept_[0])  
# MAGIC   
# MAGIC   return pd.DataFrame(results, index = [0])
# MAGIC 
# MAGIC # Get the coefficients for the train set
# MAGIC coeffs = train.groupBy().apply(linearRegressionFunction)
# MAGIC 
# MAGIC display(coeffs)

# COMMAND ----------

# MAGIC %md Note that the result is a single row of data, i.e. the three variables of the linear regression for the whole train set. Next, use them to evaluate each humidity and windspeed record of the test set. For this, you need to include the coefficients and intercept in the test set, and follow the evaluation logic to get the predicted temperature.

# COMMAND ----------

# MAGIC %md
# MAGIC Prediction of the temperature with the following model :
# MAGIC 
# MAGIC \\( predicted_{temperature} =  0.16194790218155367 \\times hum - 0.21401272509151711 \\times windspeed + 0.42338738975937296 \\)

# COMMAND ----------

# MAGIC %python
# MAGIC #Prediction of temperature 
# MAGIC #Whe use an outter join in order to create dummy columns with the coefficients. 
# MAGIC #Then, we compute de predicted temperature value using a linear equation
# MAGIC 
# MAGIC prediction_df =  test.join(coeffs,coeffs.windspeed_coeff != test.windspeed ,"outer")\
# MAGIC       .withColumn("predicted_temperature", col("intercept")+ col("hum") * col("hum_coeff")+ col("windspeed") * col("windspeed_coeff"))\
# MAGIC       .drop("hum_coeff","windspeed_coeff","intercept")
# MAGIC 
# MAGIC display(prediction_df)

# COMMAND ----------

# MAGIC %md To verify that your answer is correct, please compute the mean error (ME) of the newly created predicted temperature column for the whole test set. Remember that the ME is the average error, where the error is the difference between predicted temperature vs real temperature. In other words, the ME is a single value for the whole dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC In order to compute the error of our predicted varible, we prefered to use the mean absolute error (MAE), to avoid elimination of values by a negative sign
# MAGIC 
# MAGIC \\( MAE = \Sigma_{i = 1}^n \frac{ |y_i  -  x_i| }{n} \\)

# COMMAND ----------

# MAGIC %python
# MAGIC #We created a colum of diference between the predicted and actual temperature values, then
# MAGIC # we coputed the average of difference_abs column
# MAGIC display(
# MAGIC   prediction_df.withColumn("difference_abs", F.abs(col("predicted_temperature") - col("temp")) )\
# MAGIC                 .select(F.avg(col("difference_abs")).alias("MAE"))
# MAGIC )
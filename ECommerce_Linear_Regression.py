# Databricks notebook source
# Importing Spark sesison
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('basics').getOrCreate()
import numpy as np # Numerical Python library
import pandas as pd # Dataframe Library
import matplotlib.pyplot as plt #Plotting library
import seaborn as sns 
sns.set_theme(style='darkgrid',palette='hls')
import string

# COMMAND ----------

## Using sqlContext to query all the data from the table and load it into a spark datafrme
data = sqlContext.sql("Select * from default.ecommerce_customers_5_csv")
## Using toPandas to convert the spark dataframe into a pandas dataframe to perform operations
df = data.toPandas()

# COMMAND ----------

## Exploratory Data Analysis
## Data Wrangling/Addressing Missing Values

# COMMAND ----------

df.info()

# COMMAND ----------

df.describe()

# COMMAND ----------

sns.heatmap(df.isnull(),yticklabels=False,cbar=False,cmap='viridis')
plt.title('Missing values will be highlighted by yellow underlines')

# COMMAND ----------

## Checking Correlation and Coefficient values for all independent variables
sns.heatmap(df.corr(),annot=True)
plt.title('Correlation Coefficient')

# COMMAND ----------

## We can see that Length of Membership, Time on App and Avg. Session Length have strong positive correlations with Yearly Amount Spent

# COMMAND ----------

df.columns

# COMMAND ----------

sns.jointplot(x='Yearly Amount Spent',y='Length of Membership',data=df)
plt.title('Length of Mem. vs Yearly Spend Correlation')
plt.tight_layout()

# COMMAND ----------

sns.jointplot(x='Time on App',y='Length of Membership',data=df,kind='reg')
plt.title('Time on App vs Yearly Spend Correlation')
plt.tight_layout()

# COMMAND ----------

## Feature Consolidation / Engineering

# COMMAND ----------

## Extracting Domain from the Email address to use it as an Feature
email_temp = df['Email'].apply(lambda x: x.split('@')[1])
email_temp = pd.DataFrame(email_temp)
email_temp = email_temp['Email'].apply(lambda x: x.split('.')[0])
email_temp = pd.DataFrame(email_temp)
df['Email'] = email_temp

# COMMAND ----------

## Lets drop the categorical columns
df.drop(['Email','Address','Avatar'],axis=1,inplace=True)

# COMMAND ----------

df.head(5)

# COMMAND ----------

## Lets Standardize this data and feed it into Linear Regression algorithm
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

# COMMAND ----------

X = df.drop('Yearly Amount Spent',axis=1)
y = df['Yearly Amount Spent']

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25)

# COMMAND ----------

pipe = Pipeline([
  ('scaler',StandardScaler()),
  ('lin_reg',LinearRegression()),
])

# COMMAND ----------

pipe.fit(X_train,y_train)

# COMMAND ----------

predict_lr = pipe.predict(X_test)

# COMMAND ----------

from sklearn import metrics

# COMMAND ----------

print('MAE Score:',metrics.mean_absolute_error(y_test,predict_lr))
print('MSE Score:',metrics.mean_squared_error(y_test,predict_lr))
print('RMSE Score:',metrics.r2_score(y_test,predict_lr))

# COMMAND ----------

## Thank You
# This below Python code executed in Jupyter Notebook by [Anaconda Navigator](https://www.google.com/url?sa=t&source=web&rct=j&opi=89978449&url=https://www.anaconda.com/products/navigator&ved=2ahUKEwiT5K_m_IuNAxWce_UHHVooNSwQFnoECBkQAQ&usg=AOvVaw2FiVm4Knmhe7xplbtYwLdO) 
## We will learn to 
  1. Load an existing publicly available ['ECommerce_Dataset'](https://github.com/gaurav-bhatt89/Datasets/blob/main/Ecommerce_Customers.csv) dataset
  2. Exploratory Data Analysis
  3. Data Cleansing / Wrangling
  4. Feature Consolidation / Engineering
  5. Applying Linear Regression and evaluating its predction accuracy

### GitHub Notebook - [Link](https://github.com/gaurav-bhatt89/Databricks/blob/main/ECommerce_Linear_Regression.ipynb)
### NBViewer - [Link](https://nbviewer.org/github/gaurav-bhatt89/Databricks/blob/main/ECommerce_Linear_Regression.ipynb)
```python
# Importing Spark sesison
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('basics').getOrCreate()
import numpy as np # Numerical Python library
import pandas as pd # Dataframe Library
import matplotlib.pyplot as plt #Plotting library
import seaborn as sns 
sns.set_theme(style='darkgrid',palette='hls')
import string

## Using sqlContext to query all the data from the table and load it into a spark datafrme
data = sqlContext.sql("Select * from default.ecommerce_customers_5_csv")
## Using toPandas to convert the spark dataframe into a pandas dataframe to perform operations
df = data.toPandas()

## Exploratory Data Analysis
## Data Wrangling/Addressing Missing Values

df.info()
```
![image](https://github.com/user-attachments/assets/5329516f-2672-430a-8b7a-92445d4be735)
```python
df.describe()
```
![image](https://github.com/user-attachments/assets/24f383b0-1093-44d9-8e2a-7e2f268f517b)
```python
sns.heatmap(df.isnull(),yticklabels=False,cbar=False,cmap='viridis')
plt.title('Missing values will be highlighted by yellow underlines')
```
![image](https://github.com/user-attachments/assets/49d47b4f-a43c-47bb-9335-45a2bf0ab802)
```python
## Checking Correlation and Coefficient values for all independent variables
sns.heatmap(df.corr(),annot=True)
plt.title('Correlation Coefficient')
```
![image](https://github.com/user-attachments/assets/332f4c1c-d346-453e-a7b9-6d56b119f917)
```python
## We can see that Length of Membership, Time on App and Avg. Session Length have strong positive correlations with Yearly Amount Spent
sns.jointplot(x='Yearly Amount Spent',y='Length of Membership',data=df)
plt.title('Length of Mem. vs Yearly Spend Correlation')
plt.tight_layout()
```
![image](https://github.com/user-attachments/assets/fb6fa731-ad3c-4124-bd2e-98be7d4d0626)
```python
sns.jointplot(x='Time on App',y='Length of Membership',data=df,kind='reg')
plt.title('Time on App vs Yearly Spend Correlation')
plt.tight_layout()
```
![image](https://github.com/user-attachments/assets/1ffc9adc-7123-44f6-a303-ed35d653e2e2)
```python
## Feature Consolidation / Engineering
## Extracting Domain from the Email address to use it as an Feature
email_temp = df['Email'].apply(lambda x: x.split('@')[1])
email_temp = pd.DataFrame(email_temp)
email_temp = email_temp['Email'].apply(lambda x: x.split('.')[0])
email_temp = pd.DataFrame(email_temp)
df['Email'] = email_temp

## Lets drop the categorical columns
df.drop(['Email','Address','Avatar'],axis=1,inplace=True)

## Lets Standardize this data and feed it into Linear Regression algorithm
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

X = df.drop('Yearly Amount Spent',axis=1)
y = df['Yearly Amount Spent']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25)

pipe = Pipeline([
  ('scaler',StandardScaler()),
  ('lin_reg',LinearRegression()),
])

pipe.fit(X_train,y_train)

predict_lr = pipe.predict(X_test)

from sklearn import metrics

print('MAE Score:',metrics.mean_absolute_error(y_test,predict_lr))
print('MSE Score:',metrics.mean_squared_error(y_test,predict_lr))
print('RMSE Score:',metrics.r2_score(y_test,predict_lr))
```
![image](https://github.com/user-attachments/assets/bf73f379-b6ad-431e-9c61-b258b7f5c348)
## Thank You









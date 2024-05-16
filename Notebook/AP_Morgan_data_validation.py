# Databricks notebook source
#get the filename
filename = 'Product.csv'
filenamewithoutEXT = filename.split('.')[0]
print(filenamewithoutEXT)

# COMMAND ----------

from pyspark.sql.functions import *

sqlDbName = 'apmorgandatabase'
dbUserName = 'posadmin'
passwordKey = 'sqlpassword'
landingFolderKey = 'sastoken'
landingFileName =filename #'Product'  #dbutils.widgets.get('Product')
databricksScopeName ='apmorganvault'
dbServerUrl = 'apmorgandataserver'
dbServerPortNumber ='1433'
storageContainer ='demo'
storageAccount='apmorganaccountstorage'
landingMountPoint ='/mnt/demo'

# COMMAND ----------

if not any(mount.mountPoint == landingMountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format(storageContainer, storageAccount), mount_point= landingMountPoint, extra_configs ={'fs.azure.sas.{}.{}.blob.core.windows.net'.format(storageContainer,storageAccount):dbutils.secrets.get(scope = databricksScopeName, key= landingFolderKey)})
    print('Mounted the storage account successfully')
else:
    print('Storage account already mounted')

# COMMAND ----------

#connect to Azure SQL DB
dbPassword = dbutils.secrets.get(scope = databricksScopeName, key= passwordKey)
serverurl = 'jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(dbServerUrl, dbServerPortNumber,sqlDbName,dbUserName)
connectionProperties = {
    'password':dbPassword,
    'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}
df = spark.read.jdbc(url = serverurl, table = 'dbo.FileDetailsFormat', properties= connectionProperties)
display(df)

# COMMAND ----------

df1 = spark.read.csv('/mnt/demo/'+filename,inferSchema=True,header=True)
display(df1)

#Rule 1
errorFlag=False
errorMessage=''
totalcount=df1.count()
print(totalcount)
distinct_count=df1.distinct().count()
print(distinct_count)

if totalcount != distinct_count:
    errorFlag=True
    errorMessage='Duplicate exists Rule no 1 failt'
    print(errorMessage)
#Rule 2
df2 = df.filter(col('FileName') ==filenamewithoutEXT).select('ColumnName','ColumnDateFormat' )
display(df2)
rows = df2.collect()
for r in rows:
    colName = r[0]
    colFormat = r[1]
    print(colName,colFormat)
    formatCount =df1.filter(to_date(colName, colFormat).isNotNull() ==True).count()
    if formatCount == totalcount:
       errorFlag = True
       errorMessage = errorMessage +' DateFormate is incorrect for {} '.format(colName)
    else:
        print('All rows are good for ', colName)
print(errorMessage)

if errorFlag:
    dbutils.fs.mv('/mnt/demo/'+filename,'/mnt/rejected/'+filename )
    dbutils.notebook.exit('{"errorFlag": "true", "errorMessage":"'+errorMessage +'"}')
else:
    dbutils.fs.mv('/mnt/demo/'+filename,'/mnt/staging/'+filename )
    dbutils.notebook.exit('{"errorFlag": "false", "errorMessage":"No error"}')


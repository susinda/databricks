# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # **Setting Up Databricks and Datalake**
# MAGIC As the first step Databricks need to be configured with Datalake
# MAGIC
# MAGIC Data lake can be configured using url, key
# MAGIC
# MAGIC However having key visible in notebooks inst the best way
# MAGIC
# MAGIC For this purpose we can use Azure KeyValut and Read that from Databricks
# MAGIC
# MAGIC You have to Create a Azre KeyValut with 'Vault Access Policy', and create a secret with a key(a anme of ur choice) and value being tghe Key of the Dtalakes Master key
# MAGIC
# MAGIC In Databrick 'Create an Azure Key Vault-backed secret scope'
# MAGIC
# MAGIC Open https://<databricks-instance>#secrets/createScope and fill the required values, use a scope like 'DatabricksAcess'

# COMMAND ----------

#As the first step Databricks need to be configured with Datalake
#Data lake can be configured using url, key
#However having key visible in notebooks inst the best way
#For this purpose we can use Azure KeyValut and Read that from Databricks
#You have to Create a Azre KeyValut with 'Vault Access Policy', and create a secret with a key(a anme of ur choice) and value being tghe Key of the Dtalakes Master key
#In Databrick 'Create an Azure Key Vault-backed secret scope'
#Open https://<databricks-instance>#secrets/createScope and fill the required values, use a scope like 'DatabricksAcess'

scopes = dbutils.secrets.listScopes()
display(scopes)
secrets = dbutils.secrets.list('DatabricksAcess')
display(secrets)
datalakeSecret = dbutils.secrets.get('DatabricksAcess', 'datalakeSecret')
display(datalakeSecret)


# COMMAND ----------

#Its now time to configure datalake
storageAccName = 'integrationdl01'
storageAccDfsKey = 'fs.azure.account.key.' + storageAccName + '.dfs.core.windows.net'
storageAccBlobKey = 'fs.azure.account.key.' + storageAccName + '.blob.core.windows.net'
storageAccSecret = datalakeSecret
containerName = 'lake'
#Set the configs
spark.conf.set(storageAccDfsKey,storageAccSecret)

# COMMAND ----------

#abfss for GEn2 might be the one used for dtalake read
dfsBaseUrl = 'abfss://' + containerName + '@' + storageAccName + '.dfs.core.windows.net'
display(dfsBaseUrl)
dbutils.fs.ls(dfsBaseUrl)

# COMMAND ----------

def MountContainer(mntPoint, mntDir):
    DATA_LAKE_SOURCE = f"wasbs://{containerName}@{storageAccName}.blob.core.windows.net/{mntDir}"
    MOUNT_POINT = mntPoint
    print(f"DATA_LAKE_SOURCE: {DATA_LAKE_SOURCE}")
    print(f"MOUNT_POINT: {MOUNT_POINT}")

    for mount in dbutils.fs.mounts():
        print(f"Mount point: {mount.mountPoint}, Target: {mount.source}")
    
    # Check if the mount point already exists
    existing_mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]
    mount_exists = MOUNT_POINT in existing_mounts

    print(f"Mount execution.")

    if mount_exists:
        print(f"Mount point '{MOUNT_POINT}' already exists.")
    else:
        dbutils.fs.mount(
            source=DATA_LAKE_SOURCE,
            mount_point=MOUNT_POINT,
            extra_configs = {storageAccBlobKey:storageAccSecret}
        )
        print(f"Mounted '{MOUNT_POINT}' successfully.")
        # Additional actions or log messages for newly mounted point

# Example usage

MountContainer(mntPoint="/mnt/raw", mntDir="Raw/")



# COMMAND ----------


# Import libraries
import requests
from pyspark.sql import SparkSession

# Step 1: Make API Request
api_url = "https://dummyjson.com/products"
response = requests.get(api_url)

if response.status_code == 200:
    json_data = response.json()
else:
    raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

# Step 2: Create Spark Session
spark = SparkSession.builder.appName("api-to-delta").getOrCreate()

# Step 3: Convert JSON to DataFrame
df = spark.read.json(spark.sparkContext.parallelize([json_data]))

# Step 4: Write to Delta Table
delta_table_path = "/mnt/raw/api_data"
df.write.format("delta").mode("overwrite").save(delta_table_path)


# COMMAND ----------

# MAGIC %md In Databricks Runtime 6.2 and below, run the `display` command to view the plot.

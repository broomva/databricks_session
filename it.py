# %%
from databricks_session import DatabricksSQLSession

# %%
spark_sql = DatabricksSQLSession()
# %%
pdf = spark_sql.query_lakehouse("select * from microchip_logs limit 10")
print(pdf.head())
# %%

from databricks_session import DatabricksSparkSession

# %%
spark = DatabricksSparkSession().get_session()

# %%
sdf = spark.read.table("microchip_logs")
print(sdf.show())
# %%

from databricks_session import DatabricksMLFlowSession

# %%
mlflow_session = DatabricksMLFlowSession().get_session()

# %%
print(mlflow_session.client.MlflowClient())
# %%

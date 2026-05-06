%pip install soda-core-spark soda-core
dbutils.library.restartPython()



%run /Users/emailisbena@gmail.com/contract_demo/common_utils



# Create widgets with default values
dbutils.widgets.text("source_path", " ")
dbutils.widgets.text("contract_path", " ")
dbutils.widgets.text("bronze_table", " ")
dbutils.widgets.text("silver_table", " ")
dbutils.widgets.text("merge_key", " ")



# These variable names must match what your main function is looking for
source_path   = dbutils.widgets.get("source_path")
contract_path = dbutils.widgets.get("contract_path")
bronze_table  = dbutils.widgets.get("bronze_table")
silver_table  = dbutils.widgets.get("silver_table")
merge_key     = dbutils.widgets.get("merge_key")



run_medallion_ingestion()
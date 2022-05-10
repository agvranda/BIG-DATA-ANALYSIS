import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('First python project').getOrCreate()
sc = spark.sparkContext
csv_file = spark.read.csv(r'C:\Users\rishi\Downloads\Spark Case Study\
                        Spark Case Study\spark-inputdata\Spark-SQL-inpatientCharges.csv',
                        sep=',', inferSchema=True, header=True)

Avg_Covered_Charges = csv_file.groupby("ProviderState").avg("AverageCoveredCharges")
Avg_Covered_Charges.show(20)
Avg_Total_Payments = csv_file.groupby("ProviderState").avg("AverageTotalPayments")
Avg_Total_Payments.show(20)
Avg_Medicare_Payment = csv_file.groupby("ProviderState").avg("AverageMedicarePayments")
Avg_Medicare_Payment.show(20)

temp = csv_file.orderBy("ProviderState")
Total_Discharge = temp.groupby("ProviderState", "DRGDefinition").sum("TotalDischarges")

result1 = Total_Discharge.sort(["ProviderState", "sum(TotalDischarges)"], ascending=[True, False])
#result1.coalesce(1).write.csv(r"C:\Users\rishi\Downloads\Answer 5.csv")
result1.show(20)

file_name1 = (r'C:\Users\rishi\Downloads\Spark Case Study\
             Spark Case Study\spark-inputdata\Spark-SQL-zipcode.csv')
file_read1 = spark.read.csv(file_name1, header='true', inferSchema='true', sep=",")

file_name2 = (r'C:\Users\rishi\Downloads\Spark Case Study\
             Spark Case Study\spark-inputdata\Spark-SQL-911.csv')
file_read2 = spark.read.csv(file_name2, header='true', inferSchema='true', sep=",")

out_join = file_read1.join(file_read2, "zip", "inner")

city_title_join = out_join[['city', 'title']]
title_count = city_title_join.groupby("city","title").count()
result2 = title_count.sort(["city", "count"], ascending = [True, False])
#result2.coalesce(1).write.csv(r"C:\Users\rishi\Downloads\Answer 5.csv")
result2.show(20)

state_title_join = out_join[['state', 'title']]
title_count = state_title_join.groupby("state","title").count()
result3 = title_count.sort(["state", "count"], ascending = [True, False])
#result3.coalesce(1).write.csv(r"C:\Users\rishi\Downloads\Answer 5.csv")
result3.show(20)

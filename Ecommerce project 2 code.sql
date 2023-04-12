--Create Hive Base table For Sales Dataset---

 CREATE EXTERNAL TABLE ecommercy_olist  
 (Id BIGINT,
 order_status VARCHAR(25),
 order_products_value DOUBLE,
 order_freight_value DOUBLE,
 order_items_qty INT, 
 customer_city VARCHAR(25),
 customer_state  VARCHAR(25), 
 customer_zip_code_prefix VARCHAR(25),
 product_name_lenght INT,
 product_description_lenght INT,
 product_photos_qty INT,
 review_score INT,
 order_purchase_timestamp VARCHAR(25),
 order_aproved_at VARCHAR(25), 
 order_delivered_customer_date VARCHAR(25))
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
;
----
---load data local 

load data INPATH '/user/adhamfcihgmail/olist_public_dataset.csv.tmp' into table ecommercy_olist;

select * from ecommercy_olist LIMIT 10;
================================================

schema = StructType([
StructField("id",IntegerType(),True) ,
StructField("order_status",StringType(),True),
StructField("order_products_value",FloatType(),True),
StructField("order_freight_value",FloatType(),True),
StructField("order_items_qty",IntegerType(),True),
StructField("customer_city",StringType(),True),
StructField("customer_state",StringType(),True) ,
StructField("customer_zip_code_prefix",IntegerType(),True),
StructField("product_name_lenght",IntegerType(),True) ,
StructField("product_description_lenght",IntegerType(),True) ,
StructField("product_photos_qty",IntegerType(),True) ,
StructField("review_score",IntegerType(),True),
StructField("order_purchase_timestamp",StringType(),True) ,
StructField("order_aproved_at",StringType(),True) ,
StructField("order_delivered_customer_date",StringType(),True)
])

df = spark.read.schema(schema).csv('/sda.csv')
sda=spark.read.load('/sda' , format='csv' , sep = ',', schema =schema)
[4:03 PM, 4/18/2022] Ahmed Hassan We: df.select ( 'order_purchase_timestamp' , from_unixtime (unix_timestamp(df.order_purchase_timestamp, 'dd/MM/yyyy HH:mm')).alias ("New date")).show()
-----------------------------------------------------------------------------------
---Create firts Insight DailySalesOrdersByCity
create external table daily_sales_by_city as 
select  customer_city,order_products_value,
replace((concat(concat('20',substring(order_purchase_timestamp,7,2)),concat(substring(order_purchase_timestamp,3,4),substring(order_purchase_timestamp,1,2)))),"/","-") as pdate
  from ecommercy_olist 
  
  select*from daily_sales_by_city limit 10
--------------------------------------------------
-----------------------------------------------------------------------------------
---Create firts Insight DailySalesOrdersByState
create  table daily_sales_by_state as 
select  customer_city,avg(review_score),avg(order_freight_value),
replace((concat(concat('20',substring(order_purchase_timestamp,7,2)),concat(substring(order_purchase_timestamp,3,4),substring(order_purchase_timestamp,1,2)))),"/","-") as pdate
  from ecommercy_olist group by customer_city limit 5 ;

select*from daily_sales_by_state limit 5


-----------------------------------------------------------------------------------
---Create firts Insight weeklySalesOrdersByCity

create table weeklySalesOrdersByCity as 
select  customer_state,order_products_value,
  weekofyear(replace((concat(concat('20',substring(order_purchase_timestamp,7,2)),concat(substring(order_purchase_timestamp,3,4),substring(order_purchase_timestamp,1,2)))),"/","-")) as pweek
from ecommercy_olist limit 10 ;
select *from weeklySalesOrdersByCity limit 10;

-----------------------------------------------------------------------------------
---Create firts Insight weeklySalesOrdersByState

create table weeklySalesOrdersByState as 
select  customer_state,order_products_value,
  weekofyear(replace((concat(concat('20',substring(order_purchase_timestamp,7,2)),concat(substring(order_purchase_timestamp,3,4),substring(order_purchase_timestamp,1,2)))),"/","-")) as pweek
from ecommercy_olist  ;
select *from weeklySalesOrdersByCity limit 10;
-------------------------------------------------------------------
--Averges Insights

select  round(avg(review_score)) as Avg_R_Score,round(avg(order_freight_value)) as Avg_O_Freight,
round (avg( (datediff((replace((concat(concat('20',substring(order_aproved_at,7,2))
		,concat(concat(substring(order_aproved_at,3,4),substring(order_aproved_at,1,2)))
		,concat(substring(order_aproved_at,9),':00') )),"/","-")),
		replace((concat(concat('20',substring(order_purchase_timestamp,7,2))
         ,concat(concat(substring(order_purchase_timestamp,3,4),substring(order_purchase_timestamp,1,2)))
		 ,concat(substring(order_purchase_timestamp,9),':00') )),"/","-")
        )))) as app_avg,
        round (avg( (datediff((replace((concat(concat('20',substring(order_delivered_customer_date,7,2))
		,concat(concat(substring(order_delivered_customer_date,3,4),substring(order_delivered_customer_date,1,2)))
		,concat(substring(order_delivered_customer_date,9),':00') )),"/","-")),
		replace((concat(concat('20',substring(order_purchase_timestamp,7,2))
         ,concat(concat(substring(order_purchase_timestamp,3,4),substring(order_purchase_timestamp,1,2)))
		 ,concat(substring(order_purchase_timestamp,9),':00') )),"/","-")
        )))) as dil_avg
from ecommercy_olist  limit 10 ;
---------------------------------------------------------
--Avg_Freight_Charges_per_City

create table city_avg_frreight as select customer_city,round(avg(order_freight_value)) as avg_frieght
from ecommercy_olist 
group by customer_city  ;
select *from city_avg_frreight  where customer_city is not null limit 10;

--------------------------------------------------------
--Load Insights Tables to Cloud 

insert overwrite directory 'hdfs://adhamfcihgmail@ip-10-0-41-79/user/hadoop/' STORED AS PARQUET select * from city_avg_frreight;
insert overwrite directory 'hdfs://adhamfcihgmail@ip-10-0-41-79/user/hadoop/' STORED AS PARQUET select * from weeklySalesOrdersByState;
insert overwrite directory 'hdfs://adhamfcihgmail@ip-10-0-41-79/user/hadoop/' STORED AS PARQUET select * from weeklySalesOrdersByCity;
insert overwrite directory 'hdfs://adhamfcihgmail@ip-10-0-41-79/user/hadoop/' STORED AS PARQUET select * from DailySalesOrdersByState;
insert overwrite directory 'hdfs://adhamfcihgmail@ip-10-0-41-79/user/hadoop/' STORED AS PARQUET select * from DailySalesOrdersByCity;

INSERT OVERWRITE DIRECTORY 's3n://bucket/directory/' select * from city_avg_frreight;
INSERT OVERWRITE DIRECTORY 's3n://bucket/directory/' select * from weeklySalesOrdersByState;
INSERT OVERWRITE DIRECTORY 's3n://bucket/directory/' select * from weeklySalesOrdersByCity;
INSERT OVERWRITE DIRECTORY 's3n://bucket/directory/' select * from DailySalesOrdersByState;
INSERT OVERWRITE DIRECTORY 's3n://bucket/directory/' select * from DailySalesOrdersByCity;

------------------------------------------------------------------
---Lambda FUNCTION Python code

--Lambda FUNCTION Python Code  

from decimal import Decimal
import json
import boto3
client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
def lambda_handler(event, context):
# Extract variables from event object
bucket = event['Records'][0]['s3']['bucket']['name']
file = event['Records'][0]['s3']['object']['key']
table = file.split('/')[0]
# Get and read the JSON file
jsonObject = client.get_object(Bucket=bucket, Key=file)
jsonFileReader = jsonObject['Body'].read().decode('utf-8').split('\n')
for jsonFile in jsonFileReader:
jsonDict = json.loads(jsonFile, parse_float=Decimal)
# Put data into DynamoDB

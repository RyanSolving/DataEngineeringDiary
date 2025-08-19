# Practice


```python
from sqlalchemy import create_engine
import pandas as pd
import pyarrow.parquet as pq
from time import time
```


```python
db_user = 'root'
db_password = 'root'
db_host = 'localhost'
db_port = '5432'
db_name = 'ny_taxi'
```


```python
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
```


```python
engine = create_engine(db_url)
```


```python
engine.connect()
```




    <sqlalchemy.engine.base.Connection at 0x17e81b14d50>




```python
#read data
df = pd.read_parquet('yellow_tripdata_2025-01.parquet')
```


```python
#Generate SQL schema from data
schema_statement = pd.io.sql.get_schema(df, name='yellow_taxi_data',con=engine)
```


```python
print(schema_statement)
```

    
    CREATE TABLE yellow_taxi_data (
    	"VendorID" INTEGER, 
    	tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
    	tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
    	passenger_count FLOAT(53), 
    	trip_distance FLOAT(53), 
    	"RatecodeID" FLOAT(53), 
    	store_and_fwd_flag TEXT, 
    	"PULocationID" INTEGER, 
    	"DOLocationID" INTEGER, 
    	payment_type BIGINT, 
    	fare_amount FLOAT(53), 
    	extra FLOAT(53), 
    	mta_tax FLOAT(53), 
    	tip_amount FLOAT(53), 
    	tolls_amount FLOAT(53), 
    	improvement_surcharge FLOAT(53), 
    	total_amount FLOAT(53), 
    	congestion_surcharge FLOAT(53), 
    	"Airport_fee" FLOAT(53), 
    	cbd_congestion_fee FLOAT(53)
    )
    
    
    


```python
parquet_file = pq.ParquetFile('yellow_tripdata_2025-01.parquet')
```


```python
batch_iterator = parquet_file.iter_batches(batch_size=100000)
```


```python
table_name = 'yello_taxi_data'
```


```python
for i, batch in enumerate(batch_iterator):
    t_start = time()
    #Convert the batch into a pandas datframe
    df_chunk = batch.to_pandas()
    df_chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    t_end = time()
    print(f'Inserted chunk {i+1}, took {t_end-t_start:.3f} seconds')
```

# Learning Notes

## Overall
The process was completely reflected a concept of ETL, which stand for Extract, Transform and Load.  
Fristly, I initialized a Postgres connection with username and password.
Secondly, I extracted a data as a parquet file format.  
Thirdly, Since the data was assumed clean, therefore, I skipped the transformation step.  
Finally, I employed `df.to_sql()` to load the data into database.

## Backed by theory
Data pipelines (ETL/ELT process) are essentital to create new value from data by analytical works. We use data pipeline when we want to **move data from diverse origins to a designed destination**. It can be achieved by 2 common processes: ETL - Extract, Transformation and Loading or ELT - Extract, Load and Transform. 

### Extract (E)
- Pull data from various sources (OLTP database, APIs, logs)
### Transform (T)
- To clean, reformat and enrich data to make it ready-to-use
### Load (L)
- Place repared data into designed location (such as warehouse, analytical platforms)

## Real-world examples:
### Case 1: Retail analytics: 
**Problem to be solved:** A retailer want to report **Sales by product, promotion, and region** 
**ETL process**: 
- Extract data from various sources such as POS, supplier feeds, online order system.
- Transform: Standardized product codes, names, types...; handle missing prices
- Load: Data is load into a star schema, and ready for analytical works

## Related to the above works: 
**Problem to be solved:** US government want to know about yellow taxi operation performance in last 5 years 
**ETL process**:
- Extract: Data was directly collected from the official website
- Transform: Correct datetime format
- Load: Data is load into OLTP Postgres database


## Why matter?
-  With a pipeline: no more manual downloads
-  With a pipeline: speed up analytics since the data's ready and up-to-date
-  With a pipeline: Enable real-time analytics -> real-time decisions making

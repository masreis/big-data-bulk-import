# A big data importing tool


```bash
time java -Xmx4g \
    -cp target/big-data-bulk-import-jar-with-dependencies.jar \
    net.marcoreis.dataimport.cassandra.CassandraBulkImport
```

## Scripts

## Environment variables

```bash
CASSANDRA_HOME=/home/marco/software/cassandra
PATH=$PATH:$CASSANDRA_HOME/bin
SSTABLELOADER_DIR=/home/marco/temp/datalake/tb_fhv
# ARQUIVO_BF=$BF_DIR/201501_BolsaFamiliaFolhaPagamento.csv
```

## Cassandra scripts for creating keyspace and table


```sql
CREATE KEYSPACE datalake
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };

```


```sql
CREATE TABLE datalake.tb_fhv 
(
	ID UUID, 
	base_licence TEXT, 
	pickup_datetime TIMESTAMP, 
	dropoff_datetime TIMESTAMP, 
	initial_location INT, 
	final_location INT, 
	shared_ride BOOLEAN, 
	PRIMARY KEY(ID)
);
```


## Command to start Cassandra

```bash
cassandra -f
```

## Command to import the data in the containing directory

```bash
time sstableloader -v -d 127.0.0.1 $SSTABLELOADER_DIR
```
1m30s


It takes about 4 minutes to complete in a notebook with 4 CPU i7 processor and 16 GB of RAM.


select * from datalake.tb_fhv;

# Reference
https://www.datastax.com/dev/blog/using-the-cassandra-bulk-loader-updated
https://docs.datastax.com/en/cassandra/3.0/cassandra/tools/toolsBulkloader.html

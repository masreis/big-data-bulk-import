# A big data importing tool

Bulk import for Cassandra and HBase

A comparison between Cassandra, HBase and MySQL


## 

```bash
CASSANDRA_HOME=/home/marco/software/apache-cassandra-3.11.3
PATH=$PATH:$CASSANDRA_HOME/bin
INPUT_DIR=/home/marco/temp/datalake/tb_fhv/
```

```
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


```bash
sstableloader -v -d nome-marquina diretorio-da-tabela

```

```bash
cassandra -f
```

```bash
time sstableloader -v -d 127.0.0.1 $INPUT_DIR
rm -rf $INPUT_DIR
```


```bash
java \
	-Djava.library.path="$CASSANDRA_HOME/lib/sigar-bin/" \
	-Xmx4g \
	-cp target/cassandra-bulk-import-jar-with-dependencies.jar \
	net.marcoreis.cassandraimport.InsertBulkBolsaFamilia \
	$ARQUIVO_BF \
	nisfavorecido,id  \
	pagamentonis \
	bf
```

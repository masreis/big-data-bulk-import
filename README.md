Application for bulk importing in Cassandra


java -Djava.library.path="/home/marco/software/apache-cassandra-3.7/lib/sigar-bin/" -Xmx4g -cp target/cassandra-bulk-import-jar-with-dependencies.jar net.marcoreis.cassandraimport.InsertBulkBolsaFamilia  /home/marco/dados/bolsa-familia/entrada/201501_BolsaFamiliaFolhaPagamento.csv nis_beneficiario  bfsnis scalability


java -Djava.library.path=/home/marco/software/apache-cassandra-3.7/lib/sigar-bin/ \
-Xmx4g -cp ~/temp/cassandra-bulk-import-jar-with-dependencies.jar \
net.marcoreis.cassandraimport.InsertBulkBolsaFamilia \
/home/marco/dados/executivo/entrada/201501_BolsaFamiliaFolhaPagamento.csv \
nome_municipio bfscity scalability



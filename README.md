Application for bulk importing in Cassandra

CASSANDRA_HOME=/data/disk1/apache-cassandra-3.11.0

java \
	-Djava.library.path="$CASSANDRA_HOME/lib/sigar-bin/" \
	-Xmx4g \
	-cp target/cassandra-bulk-import-jar-with-dependencies.jar \
	net.marcoreis.cassandraimport.InsertBulkBolsaFamilia \
	/data/disk1/executivo/201501_BolsaFamiliaFolhaPagamento.csv \
	nisfavorecido,id  \
	pagamentonis \
	bf

java -Djava.library.path=/home/marco/software/apache-cassandra-3.7/lib/sigar-bin/ \
-Xmx4g -cp ~/temp/cassandra-bulk-import-jar-with-dependencies.jar \
net.marcoreis.cassandraimport.InsertBulkBolsaFamilia \
/home/marco/dados/executivo/entrada/201501_BolsaFamiliaFolhaPagamento.csv \
nome_municipio bfscity scalability

java \
-Djava.library.path="/home/marco/software/apache-cassandra-3.7/lib/sigar-bin/" -Xmx4g \
-cp target/cassandra-bulk-import-jar-with-dependencies.jar \
net.marcoreis.cassandraimport.InsertBulkBolsaFamilia \
/home/marco/dados/bolsa-familia/entrada/201501_BolsaFamiliaFolhaPagamento.csv \
"(uf, nome_municipio, mes_ano), valor_pago, nome_beneficiario" bfsvalue scalability

(uf, mes_ano), valor_pago, nome_beneficiario


bin/sstableloader -v -d nome-marquina diretorio-da-tabela

bin/sstableloader -v -d 127.0.0.1 /home/marco/bf/pagamentonis


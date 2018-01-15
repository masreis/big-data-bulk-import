Application for bulk importing in Cassandra

CASSANDRA_HOME=/data/disk1/apache-cassandra-3.11.0
BF_DIR=/data/disk1/executivo
ARQUIVO_BF=$BF_DIR/201501_BolsaFamiliaFolhaPagamento.csv

java \
	-Djava.library.path="$CASSANDRA_HOME/lib/sigar-bin/" \
	-Xmx4g \
	-cp target/cassandra-bulk-import-jar-with-dependencies.jar \
	net.marcoreis.cassandraimport.InsertBulkBolsaFamilia \
	$ARQUIVO_BF \
	nisfavorecido,id  \
	pagamentonis \
	bf

java \
-Djava.library.path="/home/marco/software/apache-cassandra-3.7/lib/sigar-bin/" -Xmx4g \
-cp target/cassandra-bulk-import-jar-with-dependencies.jar \
net.marcoreis.cassandraimport.InsertBulkBolsaFamilia \
/home/marco/dados/bolsa-familia/entrada/201501_BolsaFamiliaFolhaPagamento.csv \
"(uf, nome_municipio, mes_ano), valor_pago, nome_beneficiario" bfsvalue scalability

bin/sstableloader -v -d nome-marquina diretorio-da-tabela

bin/sstableloader -v -d 127.0.0.1 /home/marco/bf/pagamentonis


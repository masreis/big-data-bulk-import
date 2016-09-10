package net.marcoreis.cassandraimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.datastax.driver.core.utils.UUIDs;

public class InsertBulkBolsaFamilia {
	private static Logger logger = Logger.getLogger(InsertBulkBolsaFamilia.class);

	public static void main(String[] args) throws IOException {
		if (args.length < 4) {
			String msg = "Attention: Input file, primary key, table name and keyspace are obligatory.";
			System.out.println(msg);
			return;
		}
		String arquivo = args[0]; // "/home/marco/dados/executivo/entrada/201511_BolsaFamiliaFolhaPagamento.csv";
		String pk = args[1]; // "nis_beneficiario, id";
		String table = args[2];// "bfsnis";
		String keyspace = args[3]; // "scalability";
		String saida = System.getProperty("user.home") + "/" + keyspace + "/" + table;
		//
		File fSaida = new File(saida);
		FileUtils.deleteDirectory(fSaida);
		fSaida.mkdirs();
		logger.info("Output Directory: " + fSaida);
		new InsertBulkBolsaFamilia().inserir(arquivo, saida, keyspace, table, pk);
	}

	public void inserir(String filename, String outputDir, String keyspace, String table, String pk)
			throws IOException {
		Config.setClientMode(true);
		CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
		String INSERT_STMT = "INSERT INTO %s.%s (ID,UF,CODIGO_MUNICIPIO,NOME_MUNICIPIO,NIS_BENEFICIARIO,NOME_BENEFICIARIO,VALOR_PAGO,MES_ANO)"
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		String CREATE_TABLE = "CREATE TABLE %s.%s (ID TIMEUUID, UF TEXT, CODIGO_MUNICIPIO TEXT, NOME_MUNICIPIO TEXT, NIS_BENEFICIARIO TEXT, NOME_BENEFICIARIO TEXT, VALOR_PAGO FLOAT, MES_ANO TEXT, PRIMARY KEY (%s));";
		String formatedCreateTable = String.format(CREATE_TABLE, keyspace, table, pk);
		logger.info("Script for create table: " + formatedCreateTable);
		builder.inDirectory(outputDir).forTable(formatedCreateTable).using(String.format(INSERT_STMT, keyspace, table))
				.withPartitioner(new Murmur3Partitioner());
		CQLSSTableWriter writer = builder.build();

		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(filename), Charset.forName("ISO-8859-1")))) {

			int i = 0;
			reader.readLine();
			String line;
			while ((line = reader.readLine()) != null) {
				List<String> colunas = Arrays.asList(line.split("\t"));

				if (i % 1000000 == 0 && i > 0) {
					logger.info("Parcial: " + i);
				}
				float valor = Float.parseFloat(colunas.get(10).replaceAll(",", "").replaceAll("\\.00", ""));
				writer.addRow(UUIDs.timeBased(), colunas.get(0), colunas.get(1), colunas.get(2), colunas.get(7),
						colunas.get(8), valor, colunas.get(11));
				i++;
			}
			logger.info("It's over. Total inserted: " + i);
		} catch (Exception e) {
			e.printStackTrace();
		}
		writer.close();
	}
}

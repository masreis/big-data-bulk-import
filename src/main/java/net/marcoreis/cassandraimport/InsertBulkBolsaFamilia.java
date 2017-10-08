package net.marcoreis.cassandraimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
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
	private static Logger logger =
			Logger.getLogger(InsertBulkBolsaFamilia.class);

	public static void main(String[] args) throws IOException {
		if (args.length < 4) {
			String msg =
					"Attention: Input file, primary key, table name and keyspace are obligatory.";
			System.out.println(msg);
			return;
		}
		String arquivo = args[0];
		String pk = args[1];
		String table = args[2];
		String keyspace = args[3];
		String saida = System.getProperty("user.home") + "/"
				+ keyspace + "/" + table;
		//
		File fSaida = new File(saida);
		FileUtils.deleteDirectory(fSaida);
		fSaida.mkdirs();
		new InsertBulkBolsaFamilia().inserir(arquivo, saida,
				keyspace, table, pk);
		logger.info("Output Directory: " + fSaida);
	}

	public void inserir(String filename, String outputDir,
			String keyspace, String table, String pk)
			throws IOException {
		Config.setClientMode(true);
		CQLSSTableWriter.Builder builder =
				CQLSSTableWriter.builder();
		String INSERT_STMT =
				"INSERT INTO %s.%s (ID, UF , CodSiafiMunicipio , "
						+ "NomeMunicipio , CodFuncao , CodSubFuncao ,"
						+ " CodPrograma , CodAcao , NISFavorecido ,"
						+ " NomeFavorecido , FonteFinalidade ,"
						+ " ValorParcela , MesCompetencia ) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String CREATE_TABLE = "CREATE TABLE %s.%s (ID UUID, "
				+ "UF TEXT, CodSiafiMunicipio INT, "
				+ "NomeMunicipio TEXT, CodFuncao INT, "
				+ "CodSubFuncao INT, CodPrograma INT, "
				+ "CodAcao INT, NISFavorecido TEXT, "
				+ "NomeFavorecido TEXT, FonteFinalidade TEXT, "
				+ "ValorParcela DECIMAL, MesCompetencia TEXT, "
				+ "PRIMARY KEY (%s));";
		String formatedCreateTable =
				String.format(CREATE_TABLE, keyspace, table, pk);
		builder.inDirectory(outputDir)
				.forTable(formatedCreateTable)
				.using(String.format(INSERT_STMT, keyspace,
						table))
				.withPartitioner(new Murmur3Partitioner());
		CQLSSTableWriter writer = builder.build();

		try (BufferedReader reader =
				new BufferedReader(new InputStreamReader(
						new FileInputStream(filename),
						Charset.forName("ISO-8859-1")))) {
			int i = 0;
			reader.readLine();
			String line;
			while ((line = reader.readLine()) != null) {
				List<String> colunas =
						Arrays.asList(line.split("\t"));

				if (i % 1000000 == 0 && i > 0) {
					logger.info("Parcial: " + i);
				}
				float valor = Float.parseFloat(
						colunas.get(10).replaceAll(",", "")
								.replaceAll("\\.00", ""));
				writer.addRow(UUIDs.timeBased(), colunas.get(0),
						Integer.parseInt(colunas.get(1)),
						colunas.get(2),
						Integer.parseInt(colunas.get(3)),
						Integer.parseInt(colunas.get(4)),
						Integer.parseInt(colunas.get(5)),
						Integer.parseInt(colunas.get(6)),
						colunas.get(7), colunas.get(8),
						colunas.get(9), new BigDecimal(valor),
						colunas.get(11));
				i++;
			}
			logger.info("It's over. Total inserted: " + i);
			logger.info("Script for create table: "
					+ formatedCreateTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
		writer.close();
	}
}

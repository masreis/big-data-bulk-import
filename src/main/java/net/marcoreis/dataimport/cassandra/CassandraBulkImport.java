package net.marcoreis.dataimport.cassandra;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;

import com.datastax.driver.core.utils.UUIDs;

public class CassandraBulkImport {
    private static Logger logger = Logger.getLogger(CassandraBulkImport.class);
    private int importedRecords = 0;

    public void bulkImport(String inputFile, String outputDir) throws IOException {
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Config.setClientMode(true);
	String keyspace = "datalake";
	String tableName = "tb_fhv";
	String INSERT_STMT = "INSERT INTO datalake.tb_fhv "
		+ "(ID, base_licence, pickup_datetime, dropoff_datetime, initial_location, final_location, shared_ride) "
		+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
	String CREATE_TABLE = "CREATE TABLE datalake.tb_fhv "
		+ "(ID UUID, base_licence TEXT, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, initial_location INT, final_location INT, shared_ride BOOLEAN, "
		+ "PRIMARY KEY(ID))";
	CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
	builder.inDirectory(outputDir).forTable(CREATE_TABLE).using(String.format(INSERT_STMT, keyspace, tableName))
		.withPartitioner(new Murmur3Partitioner());
	CQLSSTableWriter writer = builder.build();
	//
	try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
	    reader.readLine();
	    String line;
	    while ((line = reader.readLine()) != null) {
		line = StringUtils.remove(line, "\"");
		List<String> colunas = Arrays.asList(line.split(",", -1));
		UUID uuid = UUIDs.timeBased();
		String base_licence = colunas.get(0);
		Date pickup = sdf.parse(colunas.get(1));
		Date dropoff = sdf.parse(colunas.get(2));
		Integer initialLocation = NumberUtils.toInt(colunas.get(3), 0);
		Integer finalLocation = NumberUtils.toInt(colunas.get(4), 0);
		Boolean sharedRide = BooleanUtils.toBooleanObject(colunas.get(5));
		writer.addRow(uuid, base_licence, pickup, dropoff, initialLocation, finalLocation, sharedRide);
		importedRecords++;
	    }
	    logger.info("Imported records: " + importedRecords);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	writer.close();
    }

    public int getImportedRecords() {
	return importedRecords;
    }
}

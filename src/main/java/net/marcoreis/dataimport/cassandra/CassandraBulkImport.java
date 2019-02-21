package net.marcoreis.dataimport.cassandra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;

import com.datastax.driver.core.utils.UUIDs;

public class CassandraBulkImport {
    private static Logger logger = Logger.getLogger(CassandraBulkImport.class);
    private int importedRecords = 0;
    private String INSERT_STMT = "INSERT INTO datalake.tb_fhv "
	    + "(ID, base_licence, pickup_datetime, dropoff_datetime, initial_location, final_location, shared_ride) "
	    + "VALUES (?, ?, ?, ?, ?, ?, ?)";
    private String CREATE_TABLE = "CREATE TABLE datalake.tb_fhv "
	    + "(ID UUID, base_licence TEXT, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, initial_location INT, final_location INT, shared_ride BOOLEAN, "
	    + "PRIMARY KEY(ID, base_licence))";
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static String keyspace = "datalake";
    private static String tableName = "tb_fhv";
    private CQLSSTableWriter writer;
    private String inputFile;

    public CassandraBulkImport(String inputFile, String outputDir) {
	this.inputFile = inputFile;
	DatabaseDescriptor.clientInitialization();
	CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
	builder.inDirectory(outputDir).forTable(CREATE_TABLE).using(String.format(INSERT_STMT, keyspace, tableName))
		.withPartitioner(new Murmur3Partitioner());
	writer = builder.build();
    }

    public void bulkImport() throws IOException {
	try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
	    // Discard the header
	    reader.readLine();
	    String line;
	    while ((line = reader.readLine()) != null) {
		// Prepare the columns
		line = StringUtils.remove(line, "\"");
		List<String> columns = Arrays.asList(line.split(",", -1));
		UUID uuid = UUIDs.timeBased();
		String baseLicence = columns.get(0);
		Date pickup = sdf.parse(columns.get(1));
		Date dropoff = sdf.parse(columns.get(2));
		Integer initialLocation = NumberUtils.toInt(columns.get(3), 0);
		Integer finalLocation = NumberUtils.toInt(columns.get(4), 0);
		Boolean sharedRide = "1".equals(columns.get(5));
		// Write the record
		writer.addRow(uuid, baseLicence, pickup, dropoff, initialLocation, finalLocation, sharedRide);
		importedRecords++;
	    }
	    logger.info("Imported records: " + importedRecords);
	} catch (Exception e) {
	    logger.error(e);
	}
	writer.close();
    }

    public static void main(String[] args) throws IOException {
	String inputFile = "/home/marco/dados/nyc/fhv_tripdata_2018-06.csv";
	String outputDir = System.getProperty("user.home") + "/temp/" + keyspace + "/" + tableName;
	File file = new File(outputDir);
	FileUtils.deleteDirectory(file);
	file.mkdirs();
	//
	CassandraBulkImport cassandraBulkImport = new CassandraBulkImport(inputFile, outputDir);
	cassandraBulkImport.bulkImport();
    }
}

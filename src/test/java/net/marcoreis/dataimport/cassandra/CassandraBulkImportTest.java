package net.marcoreis.dataimport.cassandra;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class CassandraBulkImportTest {

    @Test
    public void testBulkImport() throws IOException {
	String inputFile = "/home/marco/dados/nyc/fhv_tripdata_2018-01.csv";
	String outputDir = System.getProperty("user.home") + "/temp/datalake/tb_fhv/";
	File file = new File(outputDir);
	FileUtils.deleteDirectory(file);
	file.mkdirs();
	//
	CassandraBulkImport cassandraBulkImport = new CassandraBulkImport();
	cassandraBulkImport.bulkImport(inputFile, outputDir);
	assertTrue(cassandraBulkImport.getImportedRecords() > 0);
    }

}

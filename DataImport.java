import java.io.*;
import java.util.*;
import java.nio.charset.*;
import java.nio.file.*;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class DataImport {

    static String filename;

    // Number of columns & names
    static int numCols = 12;
    static String colNames[] = {"Col1", "Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9", "Col10", "Col11", "Col12"};

    public static void main(String[] args) throws IOException, InvalidRequestException {

        if (args.length < 5)
        {
            System.out.println("Expecting 5 arguments - <keyspace> <column> <schema_file> <insert_file> <csv_file>");
            System.exit(1);
        }

        long start = System.currentTimeMillis();

        String keyspace = args[0];
        String col = args[1];
        filename = args[4];

	BufferedReader reader = new BufferedReader(new FileReader(filename));

        File directory = new File(keyspace);
        if (!directory.exists())
            directory.mkdir();

	String schema = readFile(args[2], StandardCharsets.UTF_8);
	String insert = readFile(args[3], StandardCharsets.UTF_8);
	
	// Creates a new writer. You need to provide at least the directory where to write the created sstable,
	// the schema for the sstable to write and a (prepared) insert statement to use. If you do not use the
	// default partitioner (Murmur3Partitioner), you will also need to provide the partitioner in use, see
	// CQLSSTableWriter.Builder for more details on the available options.
	CQLSSTableWriter writer = CQLSSTableWriter.builder()
	    .inDirectory(keyspace + '/' + col)
	    .forTable(schema)
	    .using(insert).build();

	String line;
	int lineNumber = 0;
	CsvParse entry = new CsvParse();
	
	long timestamp = System.currentTimeMillis() * 1000;

        while ((line = reader.readLine()) != null) {	
            // Parse & Add Values
            entry.parse(line, ++lineNumber);

	    writer.addRow((Object[]) entry.d);

            // Print nK
            if (lineNumber % 10000 == 0) {
              System.out.println((lineNumber / 1000) + "K");
            }
        }

        long end = System.currentTimeMillis();

        System.out.println("Successfully parsed " + lineNumber + " lines.");
        System.out.println("Execution time was "+(end-start)+" ms.");

	// Close the writer, finalizing the sstable
	writer.close();

        System.exit(0);
    }

  static String readFile(String path, Charset encoding) 
  throws IOException 
    {
	byte[] encoded = Files.readAllBytes(Paths.get(path));
	return new String(encoded, encoding);
    }


    static class CsvParse {

        String d[] = new String[numCols];

        void parse(String line, int lineNumber) {
          String[] col = line.split(",");
          for (int j=0;j<numCols;j++) {
            d[j] = col[j].trim();
          }
        }

    }
}

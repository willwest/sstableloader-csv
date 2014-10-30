# Fork-Specific Notes
This fork of sstableloader-csv adapts the code to use CQLSSTableWriter instead of the older SSTableSimpleUnsortedWriter. With these changes, tables can be bulk inserted into Cassandra without the use of cassandra-cli. You must change the schema and insertion parameters within the code for your specific needs. A test csv file is provided that should work with this code out of the box.

# sstableloader-csv

This tool parses CSV files into sstables for Apache Cassandra. It was written for a project involving the need to parse ~75 Million row CSV files.

### Installation

Run the 'run' script with 3 arguments - `<keyspace>` `<column>` `<input>`

For example:

       ./run.sh test data test.csv

### Requirements

* $CASSANDRA_HOME must be set to the location of the apache-cassandra.jar & associated libraries.
* $CASSANDRA_CONFIG must be set to the location of your cassandra.yml config.

### Note

The number of columns & names are coded in DataImport.java, you'll need to change these yourself. There isn't also any error handling in the parsing, the data I was handling was known to be error-free.

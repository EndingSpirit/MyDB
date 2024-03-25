package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.executor.Executor;
import ed.inf.adbs.lightdb.utils.Catalog;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		// Load the database directory, input file and output file
		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Set the database directory, input file and output file
		Config.getInstance().setDbPath(databaseDir);
		Config.getInstance().setInputFilePath(inputFile);
		Config.getInstance().setOutputFilePath(outputFile);

		// Load the schema
		Catalog.getInstance().loadSchema(databaseDir + "/schema.txt");

		// Execute the query
		Executor.execute();
	}
}

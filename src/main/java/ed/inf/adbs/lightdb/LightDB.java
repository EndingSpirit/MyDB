package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.executor.Executor;
import ed.inf.adbs.lightdb.utils.DatabaseSchema;

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

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];
		Config.getInstance().setDbPath(databaseDir);
		Config.getInstance().setInputFilePath(inputFile);
		Config.getInstance().setOutputFilePath(outputFile);
		// Just for demonstration, replace this function call with your logic
		DatabaseSchema.getInstance().loadSchema(databaseDir + "/schema.txt");
		Executor.execute();
	}
}

package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.utils.Config;
import ed.inf.adbs.lightdb.executor.Executor;
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
		Executor executor = new Executor();
		executor.parse();
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

//	public static void parsingExample(String filename) {
//		try {
//			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
////            Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats");
//			if (statement != null) {
//				System.out.println("Read statement: " + statement);
//				Select select = (Select) statement;
//				System.out.println("Select body is " + select.getSelectBody());
//			}
//		} catch (Exception e) {
//			System.err.println("Exception occurred during parsing");
//			e.printStackTrace();
//		}
//	}
}

package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Catalog;
import ed.inf.adbs.lightdb.utils.Tuple;
import ed.inf.adbs.lightdb.utils.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * ScanOperator is used to read all data from a table file.
 */
public class ScanOperator extends Operator {

    private final String tableName;
    private BufferedReader reader;
    private final String databaseDir = Config.getInstance().getDbPath();

    /**
     * Constructor for ScanOperator.
     * @param tableName The name of the table to be scanned.
     */
    public ScanOperator(String tableName) throws IOException {
        this.tableName = Catalog.getInstance().resolveTableName(tableName.split(" ")[0]);
        String databaseDir = Config.getInstance().getDbPath();

        // Use the paths.get method to build Paths securely
        Path path = Paths.get(databaseDir, "data", this.tableName + ".csv");
        this.reader = new BufferedReader(new FileReader(path.toFile()));

    }

    /**
     * Gets the next tuple.
     * @return Next tuple，if no more tuple return null。
     */
    @Override
    public Tuple getNextTuple() {
        try {
            String currentLine;
            if ((currentLine = reader.readLine()) != null) {
                String[] values = currentLine.split(", ");
                List<Integer> fields = new ArrayList<>(values.length);
                for (String value : values) {
                    fields.add(Integer.parseInt(value));
                }
                return new Tuple(fields);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file: " + e.getMessage());
        }
    }

    /**
     * Reset the operator to the beginning of the table.
     */
    @Override
    public void reset() {
        try {
            reader.close();
            reader = new BufferedReader(new FileReader(databaseDir+"/data/" + tableName + ".csv"));
        } catch (IOException e) {
            throw new RuntimeException("Error resetting ScanOperator: " + e.getMessage());
        }
    }

}

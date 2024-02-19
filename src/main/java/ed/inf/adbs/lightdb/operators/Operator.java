package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Tuple;
import ed.inf.adbs.lightdb.utils.Config;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Operator is the base class for all operators in the query plan.
 */
public abstract class Operator {

    /**
     * Gets the next tuple.
     * @return Next tuple，if no more tuple return null。
     */
    public abstract Tuple getNextTuple();

    public abstract void reset();

    /**
     * Dumps the output of the operator to a file.
     * @throws IOException
     */
    public void dump() throws IOException {
        String outputFile = Config.getInstance().getOutputFilePath();
        File file = new File(outputFile);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
        PrintStream printStream = new PrintStream(outputFile);
        Tuple line = null;
        while ((line = this.getNextTuple()) != null){
            String lineStr = line.toString();
            if (lineStr.length() > 1) { // remove brackets
                String lineWithoutBrackets = lineStr.substring(1, lineStr.length() - 1);
                printStream.append(lineWithoutBrackets + "\n");
                System.out.println(lineWithoutBrackets);
            }
        }
    }

}


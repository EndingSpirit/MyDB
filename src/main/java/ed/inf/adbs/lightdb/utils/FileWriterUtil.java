package ed.inf.adbs.lightdb.utils;

import ed.inf.adbs.lightdb.model.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

public class FileWriterUtil {
    public static void writeTuplesToFile(List<Tuple> tuples) throws IOException {
        String outputDir = Config.getInstance().getOutputFilePath();
        File file = new File(outputDir);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (Tuple tuple : tuples) {
                String tupleString = tuple.toString();
                writer.write(tupleString.substring(1, tupleString.length() - 1));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error writing tuples to file: " + e.getMessage());
        }

    }
}


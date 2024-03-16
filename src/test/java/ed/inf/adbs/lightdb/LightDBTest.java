package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for LightDB.
 */
public class LightDBTest {
	@Test
	public void testMain() throws Exception {
		assertTrue(compare("1"));
		assertTrue(compare("2"));
		assertTrue(compare("3"));
		assertTrue(compare("4"));
		assertTrue(compare("5"));
		assertTrue(compare("6"));
		assertTrue(compare("13"));
		assertTrue(compare("14"));
		assertTrue(compare("15"));
		assertTrue(compare("16"));
		assertTrue(compare("17"));
		assertTrue(compare("18"));
		assertTrue(compare("19"));
		assertTrue(compare("20"));
		assertTrue(compare("21"));
		assertTrue(compare("22"));
	}

	public boolean compare(String num) throws Exception {
		String databaseDir = "db";
		String inputFile = "input/query"+num+".sql";
		String outputFile = "output/query"+num+".csv";
		String expectedOutputFile = "expected_output/query"+num+".csv";

		String[] args = new String[]{databaseDir, inputFile, outputFile};

		LightDB.main(args);

		return compareFileContent(new File(outputFile), new File(expectedOutputFile));
	}
	private boolean compareFileContent(File file1, File file2) throws Exception {
        try (BufferedReader br1 = new BufferedReader(new FileReader(file1)); BufferedReader br2 = new BufferedReader(new FileReader(file2))) {

            String line1;
            String line2;

            while ((line1 = br1.readLine()) != null) {
                line2 = br2.readLine();
                if (!line1.equals(line2)) {
                    return false;
                }
            }

            return br2.readLine() == null;
        }
	}
}

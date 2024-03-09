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
		assertTrue(compare("5"));
		assertTrue(compare("13"));
		assertTrue(compare("14"));
		assertTrue(compare("15"));
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
		BufferedReader br1 = null;
		BufferedReader br2 = null;
		try {
			br1 = new BufferedReader(new FileReader(file1));
			br2 = new BufferedReader(new FileReader(file2));

			String line1;
			String line2;

			while ((line1 = br1.readLine()) != null) {
				line2 = br2.readLine();
				if (line2 == null || !line1.equals(line2)) {
					return false;
				}
			}

			if (br2.readLine() != null) {
				return false;
			}

			return true;
		} finally {
			if (br1 != null) br1.close();
			if (br2 != null) br2.close();
		}
	}
}

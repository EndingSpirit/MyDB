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
	}

	@Test
	public void testMain2() throws Exception {
		assertTrue(compare("2"));
	}

	@Test
	public void testMain3() throws Exception {
		assertTrue(compare("3"));
	}

	@Test
	public void testMain4() throws Exception {
		assertTrue(compare("4"));
	}

	@Test
	public void testMain5() throws Exception {
		assertTrue(compare("5"));
	}

	@Test
	public void testMain6() throws Exception {
		assertTrue(compare("6"));
	}

//	@Test
//	public void testMain7() throws Exception {
//		assertTrue(compare("7"));
//	}
//
//	@Test
//	public void testMain8() throws Exception {
//		assertTrue(compare("8"));
//	}
//
//	@Test
//	public void testMain9() throws Exception {
//		assertTrue(compare("9"));
//	}
//
//	@Test
//	public void testMain10() throws Exception {
//		assertTrue(compare("10"));
//	}
//
//	@Test
//	public void testMain11() throws Exception {
//		assertTrue(compare("11"));
//	}
//
//	@Test
//	public void testMain12() throws Exception {
//		assertTrue(compare("12"));
//	}
//
	@Test
	public void testMain13() throws Exception {
		assertTrue(compare("13"));
	}

	@Test
	public void testMain14() throws Exception {
		assertTrue(compare("14"));
	}

	@Test
	public void testMain15() throws Exception {
		assertTrue(compare("15"));
	}

	@Test
	public void testMain16() throws Exception {
		assertTrue(compare("16"));
	}

	@Test
	public void testMain17() throws Exception {
		assertTrue(compare("17"));
	}

	@Test
	public void testMain18() throws Exception {
		assertTrue(compare("18"));
	}

	@Test
	public void testMain19() throws Exception {
		assertTrue(compare("19"));
	}

	@Test
	public void testMain20() throws Exception {
		assertTrue(compare("20"));
	}

	@Test
	public void testMain21() throws Exception {
		assertTrue(compare("21"));
	}

	@Test
	public void testMain22() throws Exception {
		assertTrue(compare("22"));
	}

	@Test
	public void testMain26() throws Exception {
		assertTrue(compare("26"));
	}

	@Test
	public void testMain27() throws Exception {
		assertTrue(compare("27"));
	}

	@Test
	public void testMain28() throws Exception {
		assertTrue(compare("28"));
	}

	@Test
	public void testMain32() throws Exception {
		assertTrue(compare("32"));
	}

	@Test
	public void testMain33() throws Exception {
		assertTrue(compare("33"));
	}

	@Test
	public void testMain34() throws Exception {
		assertTrue(compare("34"));
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

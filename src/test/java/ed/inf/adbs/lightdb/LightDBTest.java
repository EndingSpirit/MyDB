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

	@Test
	public void testMain7() throws Exception {
		assertTrue(compare("7"));
	}

	@Test
	public void testMain8() throws Exception {
		assertTrue(compare("8"));
	}

	@Test
	public void testMain9() throws Exception {
		assertTrue(compare("9"));
	}

	@Test
	public void testMain10() throws Exception {
		assertTrue(compare("10"));
	}

	@Test
	public void testMain11() throws Exception {
		assertTrue(compare("11"));
	}

	@Test
	public void testMain12() throws Exception {
		assertTrue(compare("12"));
	}

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
	public void testMain23() throws Exception {
		assertTrue(compare("23"));
	}

	@Test
	public void testMain24() throws Exception {
		assertTrue(compare("24"));
	}

	@Test
	public void testMain25() throws Exception {
		assertTrue(compare("25"));
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
	public void testMain29() throws Exception {
		assertTrue(compare("29"));
	}

	@Test
	public void testMain30() throws Exception {
		assertTrue(compare("30"));
	}

	@Test
	public void testMain31() throws Exception {
		assertTrue(compare("31"));
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

	@Test
	public void testMain35() throws Exception {
		assertTrue(compare("35"));
	}

	@Test
	public void testMain36() throws Exception {
		assertTrue(compare("36"));
	}

	@Test
	public void testMain37() throws Exception {
		assertTrue(compare("37"));
	}

	@Test
	public void testMain38() throws Exception {
		assertTrue(compare("38"));
	}

	@Test
	public void testMain39() throws Exception {
		assertTrue(compare("39"));
	}

	@Test
	public void testMain40() throws Exception {
		assertTrue(compare("40"));
	}

	@Test
	public void testMain41() throws Exception {
		assertTrue(compare("41"));
	}

	@Test
	public void testMain42() throws Exception {
		assertTrue(compare("42"));
	}

	@Test
	public void testMain43() throws Exception {
		assertTrue(compare("43"));
	}

	@Test
	public void testMain44() throws Exception {
		assertTrue(compare("44"));
	}

	@Test
	public void testMain45() throws Exception {
		assertTrue(compare("45"));
	}

	@Test
	public void testMain46() throws Exception {
		assertTrue(compare("46"));
	}
	@Test
	public void testMain47() throws Exception {
		assertTrue(compare("47"));
	}

	@Test
	public void testMain48() throws Exception {
		assertTrue(compare("48"));
	}
	@Test
	public void testMain49() throws Exception {
		assertTrue(compare("49"));
	}
	@Test
	public void testMain50() throws Exception {
		assertTrue(compare("50"));
	}
	@Test
	public void testMain51() throws Exception {
		assertTrue(compare("51"));
	}
	@Test
	public void testMain52() throws Exception {
		assertTrue(compare("52"));
	}
	@Test
	public void testMain53() throws Exception {
		assertTrue(compare("53"));
	}
	@Test
	public void testMain54() throws Exception {
		assertTrue(compare("54"));
	}
	@Test
	public void testMain55() throws Exception {
		assertTrue(compare("55"));
	}
	@Test
	public void testMain56() throws Exception {
		assertTrue(compare("56"));
	}

	@Test
	public void testMain57() throws Exception {
		assertTrue(compare("57"));
	}

	@Test
	public void testMain58() throws Exception {
		assertTrue(compare("58"));
	}

	@Test
	public void testMain59() throws Exception {
		assertTrue(compare("59"));
	}

	@Test
	public void testMain60() throws Exception {
		assertTrue(compare("60"));
	}

	@Test
	public void testMain61() throws Exception {
		assertTrue(compare("61"));
	}

	@Test
	public void testMain62() throws Exception {
		assertTrue(compare("62"));
	}

	@Test
	public void testMain63() throws Exception {
		assertTrue(compare("63"));
	}


	@Test
	public void testMain64() throws Exception {
		assertTrue(compare("64"));
	}

	@Test
	public void testMain65() throws Exception {
		assertTrue(compare("65"));
	}

	@Test
	public void testMain66() throws Exception {
		assertTrue(compare("66"));
	}
	@Test
	public void testMain67() throws Exception {
		assertTrue(compare("67"));
	}

	@Test
	public void testMain68() throws Exception {
		assertTrue(compare("68"));
	}
	@Test
	public void testMain69() throws Exception {
		assertTrue(compare("69"));
	}

	@Test
	public void testMain70() throws Exception {
		assertTrue(compare("70"));
	}

	@Test
	public void testMain71() throws Exception {
		assertTrue(compare("71"));
	}

	@Test
	public void testMain72() throws Exception {
		assertTrue(compare("72"));
	}




	public boolean compare(String num) throws Exception {
		String databaseDir = "samples/db";
		String inputFile = "samples/input/query"+num+".sql";
		String outputFile = "output/query"+num+".csv";
		String expectedOutputFile = "samples/expected_output/query"+num+".csv";

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

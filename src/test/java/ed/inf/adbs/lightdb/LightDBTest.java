package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.operators.*;
import ed.inf.adbs.lightdb.model.Tuple;
import ed.inf.adbs.lightdb.utils.DatabaseCatalog;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for LightDB.
 */
public class LightDBTest {

    @Before
	public void setUp() {
		// 初始化数据库目录路径等测试前的设置
        // 数据库目录路径
        String databaseDir = "db";
		DatabaseCatalog.getInstance().loadSchema(databaseDir + "/schema.txt");
	}

	@Test
	public void testScanOperator() throws Exception {
		ScanOperator scanOperator = new ScanOperator("Boats");
		Tuple tuple;
		boolean hasData = false;
		while ((tuple = scanOperator.getNextTuple()) != null) {
			hasData = true;
			// 对tuple进行一些断言测试
			assertNotNull(tuple);
			// 更多的断言
		}
		assertTrue(hasData); // 确保至少读取到了一些数据
	}

	@Test
	public void testSelectOperator() throws Exception {
		// 首先创建ScanOperator
		ScanOperator scanOperator = new ScanOperator("Boats");
		// 然后创建SelectOperator，这里需要一个where条件表达式
		String whereCondition = "Boats.D = 104"; // 假设A是列名之一，且我们想要筛选A=5的行
		Select selectStatement = (Select) CCJSqlParserUtil.parse("SELECT * FROM Boats WHERE " + whereCondition);
		PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
		SelectOperator selectOperator = new SelectOperator(scanOperator, plainSelect.getWhere(),"Boats");

		Tuple tuple;
		boolean conditionMet = false;
		while ((tuple = selectOperator.getNextTuple()) != null) {
			conditionMet = true;
			// 对tuple进行一些断言测试，例如确保满足where条件
			assertNotNull(tuple);
			// 这里可以添加更具体的断言，比如检查tuple的某个字段值是否等于5
		}
		assertTrue(conditionMet); // 确保至少有一行数据满足条件
	}

}

package ed.inf.adbs.lightdb.model;

import java.util.HashMap;
import java.util.Map;

public class Row {
    private Map<String, Object> columns; // 列名到列值的映射

    public Row() {
        this.columns = new HashMap<>();
    }

    public void setColumn(String columnName, Object value) {
        columns.put(columnName, value);
    }

    public Object getColumnValue(String columnName) {
        return columns.get(columnName);
    }

    // Getters 和 Setters
    public Map<String, Object> getColumns() {
        return columns;
    }
}


package ed.inf.adbs.lightdb.model;

import java.util.List;
import java.util.Map;

public class Tuple {

    private List<Integer> fields;
    private Map<String, Integer> columnToIndex;

    public Tuple(List<Integer> fields, Map<String, Integer> columnToIndex) {
        this.fields = fields;
        this.columnToIndex = columnToIndex;
    }

    public Integer getField(String columnName) {
        if (columnToIndex.containsKey(columnName)) {
            return fields.get(columnToIndex.get(columnName));
        } else {
            throw new IllegalArgumentException("Column name does not exist: " + columnName);
        }
    }

    public void setField(int index, Integer value) {
        fields.set(index, value);
    }

    @Override
    public String toString() {
        return fields.toString();
    }

}

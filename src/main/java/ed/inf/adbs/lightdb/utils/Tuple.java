package ed.inf.adbs.lightdb.utils;

import java.util.List;

public class Tuple {

    private final List<Integer> fields;

    public Tuple(List<Integer> fields) {
        this.fields = fields;
    }

    public Integer getField(int index) {
        return fields.get(index);
    }

    public List<Integer> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}

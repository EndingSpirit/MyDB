package ed.inf.adbs.lightdb.utils;

import java.util.List;

public class Tuple {

    private List<Integer> fields;

    public Tuple(List<Integer> fields) {
        this.fields = fields;
    }

    public Integer getField(int index) {
        return fields.get(index);
    }

    public List<Integer> getFields() {
        return fields;
    }

    public void setField(int index, Integer value) {
        fields.set(index, value);
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}

package ed.inf.adbs.lightdb.model;

import java.util.List;

public class Tuple {

    private List<Integer> fields;

    public Tuple(List<Integer> fields) {
        this.fields = fields;
    }

    public Integer getField(int index) {
        return fields.get(index);
    }

    public void setField(int index, Integer value) {
        fields.set(index, value);
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}

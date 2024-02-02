package ed.inf.adbs.lightdb.model;

import java.util.List;

public class Tuple {

    private List<Object> fields;

    public Tuple(List<Object> fields) {
        this.fields = fields;
    }

    public Object getField(int index) {
        return fields.get(index);
    }

    public void setField(int index, Object value) {
        fields.set(index, value);
    }

    @Override
    public String toString() {
        return fields.toString();
    }

}


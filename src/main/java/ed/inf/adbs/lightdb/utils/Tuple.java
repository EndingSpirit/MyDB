package ed.inf.adbs.lightdb.utils;

import java.util.List;
import java.util.Objects;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tuple)) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(fields, tuple.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fields);
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}

package ed.inf.adbs.lightdb.utils;

import java.util.List;
import java.util.Objects;

/**
 * Tuple class is the data structure used to store the tuple
 */
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

    /**
     * Check if the tuple is equal to another object
     * @param o The object to be compared
     * @return true if the tuple is equal to the object, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tuple)) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(fields, tuple.fields);
    }

    /**
     * Calculate the hash code of the tuple
     * @return the hash code of the tuple
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(fields);
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}

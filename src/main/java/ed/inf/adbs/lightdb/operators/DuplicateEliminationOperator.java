package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.HashSet;
import java.util.Set;

/**
 * DuplicateEliminationOperator is used to eliminate duplicate tuples
 */
public class DuplicateEliminationOperator extends Operator {
    private final PlainSelect plainSelect;
    private final Operator child;
    private final Set<Integer> seenHashes = new HashSet<>();

    /**
     * Constructor for DuplicateEliminationOperator
     * @param plainSelect The select clause
     * @param child The child operator
     */
    public DuplicateEliminationOperator(PlainSelect plainSelect, Operator child) {
        this.plainSelect = plainSelect;
        this.child = child;
    }

    @Override
    public Tuple getNextTuple() {
        if (plainSelect.getDistinct() == null) {
            return child.getNextTuple();
        }

        Tuple current;
        while ((current = child.getNextTuple()) != null) {
            // Calculates the hash value of the current tuple
            int currentHash = current.hashCode();
            // Check if you've seen the hash before
            if (!seenHashes.contains(currentHash)) {
                // If this is a new hash, it is a unique tuple
                seenHashes.add(currentHash);
                return current;
            }
            // If you've already seen it, continue the loop and work on the next tuple
        }

        return null;
    }

    @Override
    public void reset() {
        child.reset();
        seenHashes.clear();
    }
}

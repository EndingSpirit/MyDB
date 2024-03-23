package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.utils.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.HashSet;
import java.util.Set;

public class DuplicateEliminationOperator extends Operator {
    private final PlainSelect plainSelect;
    private final Operator child;
    private final Set<Integer> seenHashes = new HashSet<>();

    public DuplicateEliminationOperator(PlainSelect plainSelect, Operator child) {
        this.plainSelect = plainSelect;
        this.child = child;
    }

    @Override
    public Tuple getNextTuple() {
        // 如果查询中没有使用 DISTINCT 关键字，则直接返回子操作符的下一个元组
        if (plainSelect.getDistinct() == null) {
            return child.getNextTuple();
        }

        Tuple current;
        while ((current = child.getNextTuple()) != null) {
            // 计算当前元组的哈希值
            int currentHash = current.hashCode();
            // 检查是否已经见过这个哈希值
            if (!seenHashes.contains(currentHash)) {
                // 如果这是一个新的哈希值，说明这是一个唯一的元组
                seenHashes.add(currentHash);
                return current;
            }
            // 如果已经见过，继续循环，处理下一个元组
        }

        return null; // 没有更多元组
    }

    @Override
    public void reset() {
        child.reset();
        seenHashes.clear();
    }
}

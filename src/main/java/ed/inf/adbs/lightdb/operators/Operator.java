package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;

public abstract class Operator {

    /**
     * 获取下一个元组。
     * @return 下一个元组，如果没有更多元组则返回 null。
     */
    public abstract Tuple getNextTuple();

    public abstract void reset();

}


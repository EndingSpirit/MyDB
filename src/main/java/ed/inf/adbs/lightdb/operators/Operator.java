package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.model.Tuple;

import java.io.IOException;

public abstract class Operator {

    /**
     * 获取下一个元组。
     * @return 下一个元组，如果没有更多元组则返回 null。
     */
    public abstract Tuple getNextTuple() throws IOException;

    public abstract void reset() throws IOException;

    public void dump() throws IOException {
        Tuple line = null;
        while ((line = this.getNextTuple()) != null){
            System.out.println(line);
        }
    }

}


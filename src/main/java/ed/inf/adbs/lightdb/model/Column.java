package ed.inf.adbs.lightdb.model;

public class Column {
    private String name; // 列名
    private Class<?> type; // 列的数据类型

    public Column(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    // Getters 和 Setters
    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }
}

package ed.inf.adbs.lightdb.utils;

public class Config {
    private static Config instance;
    private String dbPath;
    private String outputFilePath;
    private String inputFilePath;

    private Boolean useAlias = false;

    private Config() {
    }

    public static Config getInstance() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public String getDbPath() {
        return dbPath;
    }

    public String getOutputFilePath() {
        return outputFilePath;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public Boolean isUseAliases() {
        return useAlias;
    }

    public void setUseAliases(Boolean useAlias) {
        this.useAlias = useAlias;
    }

    public void setOutputFilePath(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    public void setInputFilePath(String inputFilePath) {
        this.inputFilePath = inputFilePath;
    }
    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }
}


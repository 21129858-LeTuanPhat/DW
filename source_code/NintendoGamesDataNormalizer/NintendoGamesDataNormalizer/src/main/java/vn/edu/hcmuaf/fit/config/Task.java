package vn.edu.hcmuaf.fit.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Task {
    private int id;
    private String name;
    private String sourcePath;
    private String header;
    private String params;
    private String timeStart;
    private String configJson;
    private String pathSaveFile;
    private String prefixFileName;
    private String createdAt;
    private String updatedAt;
    private String description;
    private String store;
    private String type;

    @JsonProperty("dbConfig")
    private DbConfig dbConfig;

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public DbConfig getDbConfig() { return dbConfig; }
    public void setDbConfig(DbConfig dbConfig) { this.dbConfig = dbConfig; }

    public String getSourcePath() { return sourcePath; }
    public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }

    public String getHeader() { return header; }
    public void setHeader(String header) { this.header = header; }

    public String getParams() { return params; }
    public void setParams(String params) { this.params = params; }

    public String getTimeStart() { return timeStart; }
    public void setTimeStart(String timeStart) { this.timeStart = timeStart; }

    public String getConfigJson() { return configJson; }
    public void setConfigJson(String configJson) { this.configJson = configJson; }

    public String getPathSaveFile() { return pathSaveFile; }
    public void setPathSaveFile(String pathSaveFile) { this.pathSaveFile = pathSaveFile; }

    public String getPrefixFileName() { return prefixFileName; }
    public void setPrefixFileName(String prefixFileName) { this.prefixFileName = prefixFileName; }

    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }

    public String getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getStore() { return store; }
    public void setStore(String store) { this.store = store; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
}

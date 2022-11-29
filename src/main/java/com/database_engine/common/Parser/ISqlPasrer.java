package com.database_engine.common.Parser;

import java.util.List;

public interface ISqlPasrer {
    public String parseSelectTableName(String query);

    public String parseInsertTableName(String query);

    public String parseDeleteTableName(String query);

    public String parseUpdateTableName(String query);

    public List<String> parseSelectColumnName(String query);

    public List<List<String>> parseSelectStatement(String query);

    public List<List<String>> parseInsertStatement(String query);

    public List<List<String>> parseDeleteStatement(String query);

    public List<List<String>> parseUpdateStatement(String query);

}

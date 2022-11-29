package com.database_engine.entities.interfaces;

import java.util.List;

import com.database_engine.classes.Column;
import com.database_engine.classes.Row;
import com.database_engine.common.ProcessResult.ProcessResult;

public interface ITable {
    public ProcessResult insertRow(String query);

    public ProcessResult deleteRow(String query);

    public ProcessResult updateRow(String query);

    public ProcessResult searchRow(String query);

    public String getPrimaryKey();

    public String getTableName();

    public Column getTableColumns();

    public List<Row> getTableRows();

    public String getDatabaseName();

    public void setPrimaryKey(String primayKey);

    public void setTableName(String name);

    public void setTableColumns(Column column);

    public void setDatabaseName(String name);

}

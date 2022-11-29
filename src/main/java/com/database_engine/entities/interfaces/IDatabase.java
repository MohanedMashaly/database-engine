package com.database_engine.entities.interfaces;

import com.database_engine.classes.Column;
import com.database_engine.common.ProcessResult.ProcessResult;

public interface IDatabase {

    public ProcessResult dropDatabase(String name);

    public ProcessResult updateDatabase(String oldName, String newName);

    public ProcessResult createTable(String name, Column columns);

    public ProcessResult dropTable(String name);

}

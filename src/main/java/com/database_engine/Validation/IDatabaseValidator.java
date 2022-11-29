package com.database_engine.Validation;

public interface IDatabaseValidator {
    public boolean isDatabaseNameExsits(String name);

    public boolean isDatabaseNameValid(String name);

    public boolean isTableNameValid(String name);

    public boolean isTableNameExists(String databaseName, String name);
}

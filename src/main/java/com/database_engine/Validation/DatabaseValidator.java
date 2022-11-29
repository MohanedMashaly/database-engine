package com.database_engine.Validation;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.database_engine.common.Helper.DataReader;

public class DatabaseValidator implements IDatabaseValidator {

    @Override
    public boolean isDatabaseNameExsits(String name) {
        DataReader dataReader = new DataReader();
        boolean isDatabaseExsits = dataReader.getDatabaseName(name) == null ? false : true;
        return isDatabaseExsits;
    }

    @Override
    public boolean isDatabaseNameValid(String name) {
        boolean isNameValid = true;
        Pattern pattern = Pattern.compile("[0-9a-zA-Z$_]+");
        Matcher matcher = pattern.matcher(name);
        if (name.isEmpty() || !matcher.matches()) {
            isNameValid = false;
        }
        return isNameValid;
    }

    @Override
    public boolean isTableNameValid(String name) {
        boolean isNameValid = true;
        Pattern pattern = Pattern.compile("[0-9a-zA-Z$_]+");
        Matcher matcher = pattern.matcher(name);
        if (name.isEmpty() || !matcher.matches()) {
            isNameValid = false;
        }
        return isNameValid;
    }

    @Override
    public boolean isTableNameExists(String databaseName, String name) {
        DataReader dataReader = new DataReader();
        List<String> tablesNames = dataReader.getTablesName(databaseName);
        return tablesNames.contains(name);

    }

}

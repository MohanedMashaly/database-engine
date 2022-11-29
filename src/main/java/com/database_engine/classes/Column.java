package com.database_engine.classes;

import java.util.ArrayList;
import java.util.List;
import com.database_engine.entities.interfaces.IColumn;

/**
 * Column class represents all column names in a table.
 */
public class Column implements IColumn {
    public List<String> names;

    public Column(List<String> names) {
        if (names == null) {
            names = new ArrayList<String>();
        } else {
            this.names = names;
        }
    }

    public List<String> getNames() {
        return names;
    }

    public void setName(List<String> names) {
        this.names = names;
    }

}

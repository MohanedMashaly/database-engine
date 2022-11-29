package com.database_engine.classes;

import com.database_engine.entities.interfaces.IRow;

/**
 * Row class represents string of values of in a table.
 */
public class Row implements IRow {
    public String[] value;

    public Row(String[] row) {
        this.value = row;
    }

    public String[] getValue() {
        return value;
    }

    public void setValue(String[] value) {
        this.value = value;
    }

}

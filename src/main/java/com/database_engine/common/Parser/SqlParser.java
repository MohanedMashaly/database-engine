package com.database_engine.common.Parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import com.database_engine.entities.enums.EOperand;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

/**
 * SqlParser is a class that parse sql queries to perform actions on database,
 * tables.
 */

public class SqlParser implements ISqlPasrer {

    @Override
    public String parseSelectTableName(String query) {
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            Select selectStatement = (Select) statement;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
            return tableList.get(0);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String parseInsertTableName(String query) {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        try {
            Insert insert = (Insert) parserManager.parse(new StringReader(query));
            return insert.getTable().getName();
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String parseUpdateTableName(String query) {
        try {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Update update = (Update) parserManager.parse(new StringReader(query));
            return update.getTable().getName();
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String parseDeleteTableName(String query) {
        Statement statement;
        String tableName = null;
        try {
            statement = CCJSqlParserUtil.parse(query);
            Delete delete = (Delete) statement;
            tableName = delete.getTable().getName();
            return tableName;
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return tableName;
    }

    @Override
    public List<String> parseSelectColumnName(String query) {
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            Select selectStatement = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
            List<SelectItem> selectItems = plainSelect.getSelectItems();
            List<String> columns = new ArrayList<String>();
            for (int item = 0; item < selectItems.size(); item++) {
                columns.add(selectItems.get(item).toString());
            }
            return columns;
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public List<List<String>> parseSelectStatement(String query) {
        List<List<String>> output = new ArrayList<>();
        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            Select selectStatement = (Select) statement;
            Expression whereCondition = ((PlainSelect) ((Select) selectStatement).getSelectBody()).getWhere();
            if (whereCondition == null) {
                return null;
            }
            String[] expressions = whereCondition.toString().split(" ");
            List<String> columns = new ArrayList<String>();
            List<String> operators = new ArrayList<String>();
            List<String> values = new ArrayList<String>();
            List<String> logicalOperators = new ArrayList<String>();
            for (int expression = 0; expression < expressions.length - 2;) {
                if (switchToOperand(expressions[expression]) != null) {
                    logicalOperators.add(expressions[expression]);
                    expression++;
                }
                columns.add(expressions[expression]);
                operators.add(expressions[expression + 1]);
                values.add(expressions[expression + 2]);
                expression += 3;
            }
            output.add(columns);
            output.add(operators);
            output.add(values);
            output.add(logicalOperators);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return output;
    }

    @Override
    public List<List<String>> parseInsertStatement(String query) {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        List<List<String>> output = new ArrayList<>();
        try {
            Insert insert = (Insert) parserManager.parse(new StringReader(query));
            List<String> columns = new ArrayList<>();
            List<String> values = new ArrayList<>();
            for (int column = 0; column < insert.getColumns().size(); column++) {
                columns.add(insert.getColumns().get(column).getColumnName());
                values.add(((ExpressionList) insert.getItemsList()).getExpressions().get(column).toString());
            }
            output.add(columns);
            output.add(values);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return output;
    }

    @Override
    public List<List<String>> parseDeleteStatement(String query) {
        Statement statement;
        List<List<String>> output = new ArrayList<List<String>>();
        try {
            statement = CCJSqlParserUtil.parse(query);
            Delete delete = (Delete) statement;
            Expression where = delete.getWhere();
            if (where != null) {
                BinaryExpression expr = (BinaryExpression) (where);
                String[] column_conditions = expr.toString().split(" ");
                List<String> columns = new ArrayList<String>();
                List<String> operands = new ArrayList<String>();
                List<String> values = new ArrayList<String>();
                List<String> logicalOperators = new ArrayList<String>();
                for (int condition = 0; condition < column_conditions.length - 2; condition += 3) {
                    if (this.switchToOperand(column_conditions[condition]) != null) {
                        logicalOperators.add(column_conditions[condition]);
                        condition++;
                    }
                    columns.add(column_conditions[condition]);
                    operands.add(column_conditions[condition + 1]);
                    values.add(column_conditions[condition + 2]);
                }
                output.add(columns);
                output.add(operands);
                output.add(values);
                output.add(logicalOperators);

            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        return output;
    }

    @Override
    public List<List<String>> parseUpdateStatement(String query) {
        Statement statement;
        List<List<String>> output = new ArrayList<List<String>>();
        try {
            statement = CCJSqlParserUtil.parse(query);
            Update update = (Update) statement;
            List<String> updatedColumnNames = new ArrayList<String>();
            List<String> updatedColumnValues = new ArrayList<String>();
            for (int set = 0; set < update.getUpdateSets().size(); set++) {
                updatedColumnNames.add(update.getUpdateSets().get(set).getColumns().toString());
                updatedColumnValues.add(update.getUpdateSets().get(set).getExpressions().toString());
            }
            output.add(updatedColumnNames);
            output.add(updatedColumnValues);
            Expression where = update.getWhere();
            if (where != null) {
                BinaryExpression expr = (BinaryExpression) (where);
                String[] column_conditions = expr.toString().split(" ");
                List<String> columns = new ArrayList<String>();
                List<String> operands = new ArrayList<String>();
                List<String> values = new ArrayList<String>();
                List<String> logicalOperators = new ArrayList<String>();
                for (int condition = 0; condition < column_conditions.length - 2; condition += 3) {
                    if (this.switchToOperand(column_conditions[condition]) != null) {
                        logicalOperators.add(column_conditions[condition]);
                        condition++;
                    }
                    columns.add(column_conditions[condition]);
                    operands.add(column_conditions[condition + 1]);
                    values.add(column_conditions[condition + 2]);
                }
                output.add(columns);
                output.add(operands);
                output.add(values);
                output.add(logicalOperators);
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        return output;
    }

    public EOperand switchToOperand(String operand) {
        switch (operand.toUpperCase()) {
            case "AND":
                return EOperand.AND;
            case "OR":
                return EOperand.OR;
        }
        return null;
    }

}

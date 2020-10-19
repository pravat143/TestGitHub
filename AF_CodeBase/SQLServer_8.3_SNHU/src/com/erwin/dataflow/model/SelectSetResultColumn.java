
package com.erwin.dataflow.model;

import com.erwin.util.Pair;
import com.erwin.util.SQLUtil;
import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.ESetOperatorType;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.util.HashSet;
import java.util.Set;

public class SelectSetResultColumn extends ResultColumn {

    public SelectSetResultColumn(ResultSet resultSet,
                                 TResultColumn resultColumnObject, int index) {
        super(resultSet, resultColumnObject);

        if (isNotSameAlias(resultSet, index)) {
            if (resultSet instanceof SelectResultSet) {
                this.name = "UNNAMED"
                        + getIndexOf(
                        ((SelectResultSet) resultSet)
                                .getResultColumnObject(),
                        resultColumnObject);
            } else if (resultSet instanceof SelectSetResultSet) {
                this.name = "UNNAMED"
                        + getIndexOf(
                        ((SelectSetResultSet) resultSet)
                                .getResultColumnObject(),
                        resultColumnObject);
            } else
                this.name = resultColumnObject.toString();
        } else {
            if (resultColumnObject.getAliasClause() != null) {
                this.name = resultColumnObject.getAliasClause().toString();
            } else if (resultColumnObject.getExpr()
                    .getExpressionType() == EExpressionType.simple_object_name_t) {
                this.name = resultColumnObject.getColumnNameOnly();
            } else if (resultColumnObject.getFieldAttr() != null) {
                this.name = resultColumnObject.getColumnNameOnly();
            } else {
                if (resultColumnObject.getExpr()
                        .getExpressionType() == EExpressionType.simple_constant_t) {
                    this.name = resultColumnObject.toString();
                } else if (resultColumnObject.getExpr()
                        .getExpressionType() == EExpressionType.function_t) {
                    this.name = resultColumnObject.getExpr()
                            .getFunctionCall()
                            .getFunctionName()
                            .toString();
                } else if (resultColumnObject.getColumnNameOnly() != null
                        && !"".equals(
                        resultColumnObject.getColumnNameOnly())) {
                    this.name = resultColumnObject.getColumnNameOnly();
                } else {
                    this.name = resultColumnObject.toString();
                }
            }

            if (resultColumnObject.getExpr()
                    .getExpressionType() == EExpressionType.function_t) {
                this.fullName = resultColumnObject.getExpr()
                        .getFunctionCall()
                        .getFunctionName()
                        .toString();
            } else {
                this.fullName = resultColumnObject.toString();
            }
        }

        this.fullName = this.name;
        
        this.name = SQLUtil.trimColumnStringQuote(name);
    }

    private boolean isNotSameAlias(ResultSet resultSet, int index) {
        if (resultSet instanceof SelectSetResultSet) {
            SelectSetResultSet selectSetResultSet = (SelectSetResultSet) resultSet;
            if (selectSetResultSet
                    .getSetOperatorType() == ESetOperatorType.none) {
                return false;
            }
            TSelectSqlStatement select = selectSetResultSet.getSelectObject();
            return isNotSameAlias(select, index);
        } else if (resultSet instanceof SelectResultSet) {
            SelectResultSet selectResultSet = (SelectResultSet) resultSet;
            if (selectResultSet.getSelectStmt()
                    .getSetOperatorType() == ESetOperatorType.none) {
                return false;
            }
            TSelectSqlStatement select = selectResultSet.getSelectStmt();
            return isNotSameAlias(select, index);
        }
        return false;
    }

    private boolean isNotSameAlias(TSelectSqlStatement select, int index) {
        Set<String> aliasSet = new HashSet<String>();
        collectSelectAlias(select, index, aliasSet);
        return aliasSet.size() > 1;
    }

    private void collectSelectAlias(TSelectSqlStatement select, int index,
                                    Set<String> aliasSet) {
        if (select.getSetOperatorType() == ESetOperatorType.none) {
            TResultColumn column = select.getResultColumnList()
                    .getResultColumn(index);

            String alias = null;

            if (column.getAliasClause() != null) {
                alias = column.getAliasClause().toString();
            } else if (column.getColumnNameOnly() != null
                    && !"".equals(column.getColumnNameOnly())) {
                alias = column.getColumnNameOnly();
            } else {
                alias = column.toString();
            }

            aliasSet.add(alias.toUpperCase());

        } else {
            collectSelectAlias(select.getLeftStmt(), index, aliasSet);
            collectSelectAlias(select.getRightStmt(), index, aliasSet);
        }
    }

    public SelectSetResultColumn(ResultSet resultSet,
                                 ResultColumn resultColumn, int index) {
        if (resultColumn == null || resultSet == null)
            throw new IllegalArgumentException(
                    "ResultColumn arguments can't be null.");

        id = ++ModelBindingManager.get().TABLE_COLUMN_ID;

        this.resultSet = resultSet;
        resultSet.addColumn(this);

        if (resultColumn.getColumnObject() instanceof TResultColumn) {
            TResultColumn resultColumnObject = (TResultColumn) resultColumn
                    .getColumnObject();
            if (resultColumnObject.getAliasClause() != null) {
                this.alias = resultColumnObject.getAliasClause().toString();
                TSourceToken startToken = resultColumnObject.getAliasClause()
                        .getStartToken();
                TSourceToken endToken = resultColumnObject.getAliasClause()
                        .getEndToken();
                this.aliasStartPosition = new Pair<Long, Long>(
                        startToken.lineNo, startToken.columnNo);
                this.aliasEndPosition = new Pair<Long, Long>(endToken.lineNo,
                        endToken.columnNo + endToken.astext.length());

                this.name = this.alias;
            } else if (resultColumnObject.getExpr()
                    .getExpressionType() == EExpressionType.simple_object_name_t) {
                this.name = resultColumnObject.getColumnNameOnly();
            } else if (resultColumnObject.getFieldAttr() != null) {
                this.name = resultColumnObject.getColumnNameOnly();
            } else {
                if (resultColumnObject.getExpr()
                        .getExpressionType() == EExpressionType.simple_constant_t) {
                    this.name = resultColumnObject.toString();
                } else if (resultColumnObject.getExpr()
                        .getExpressionType() == EExpressionType.function_t) {
                    this.name = resultColumnObject.getExpr()
                            .getFunctionCall()
                            .getFunctionName()
                            .toString();
                } else if (resultColumnObject.getColumnNameOnly() != null
                        && !"".equals(
                        resultColumnObject.getColumnNameOnly())) {
                    this.name = resultColumnObject.getColumnNameOnly();
                } else {
                    this.name = resultColumnObject.toString();
                }
            }

            if (resultColumnObject.getExpr()
                    .getExpressionType() == EExpressionType.function_t) {
                this.fullName = resultColumnObject.getExpr()
                        .getFunctionCall()
                        .getFunctionName()
                        .toString();
            } else {
                this.fullName = resultColumnObject.toString();
            }

            TSourceToken startToken = resultColumnObject.getStartToken();
            TSourceToken endToken = resultColumnObject.getEndToken();
            this.startPosition = new Pair<Long, Long>(startToken.lineNo,
                    startToken.columnNo);
            this.endPosition = new Pair<Long, Long>(endToken.lineNo,
                    endToken.columnNo + endToken.astext.length());
            this.columnObject = resultColumnObject;
        } else if (resultColumn.getColumnObject() instanceof TObjectName) {
            TObjectName resultColumnObject = (TObjectName) resultColumn
                    .getColumnObject();

            if (resultColumnObject.getColumnNameOnly() != null
                    && !"".equals(resultColumnObject.getColumnNameOnly())) {
                this.name = resultColumnObject.getColumnNameOnly();
            } else {
                this.name = resultColumnObject.toString();
            }

            this.fullName = this.name;

            TSourceToken startToken = resultColumnObject.getStartToken();
            TSourceToken endToken = resultColumnObject.getEndToken();
            this.startPosition = new Pair<Long, Long>(startToken.lineNo,
                    startToken.columnNo);
            this.endPosition = new Pair<Long, Long>(endToken.lineNo,
                    endToken.columnNo + endToken.astext.length());
            this.columnObject = resultColumnObject;
        } else {
            this.name = resultColumn.getName();
            this.fullName = this.name;

            TSourceToken startToken = resultColumn.getColumnObject()
                    .getStartToken();
            TSourceToken endToken = resultColumn.getColumnObject()
                    .getEndToken();
            this.startPosition = new Pair<Long, Long>(startToken.lineNo,
                    startToken.columnNo);
            this.endPosition = new Pair<Long, Long>(endToken.lineNo,
                    endToken.columnNo + endToken.astext.length());
            this.columnObject = resultColumn.getColumnObject();
        }
    }

    private int getIndexOf(TResultColumnList resultColumnList,
                           TResultColumn resultColumnObject) {
        for (int i = 0; i < resultColumnList.size(); i++) {
            if (resultColumnList.getResultColumn(i) == resultColumnObject)
                return i;
        }
        return -1;
    }
}

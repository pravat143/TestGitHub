package com.erwin.dataflow.model;

import com.erwin.util.Pair;
import com.erwin.util.SQLUtil;
import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TParseTreeNode;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TResultColumnList;

import java.util.ArrayList;
import java.util.List;

public class ResultColumn {

    protected ResultSet resultSet;

    protected int id;

    protected String alias;
    protected Pair<Long, Long> aliasStartPosition;
    protected Pair<Long, Long> aliasEndPosition;

    protected String fullName;
    protected String name;

    protected Pair<Long, Long> startPosition;
    protected Pair<Long, Long> endPosition;

    protected TParseTreeNode columnObject;

    protected List<TObjectName> starLinkColumns = new ArrayList<TObjectName>();

    private boolean showStar = true;

    protected boolean isFunction = false;

    public ResultColumn() {

    }

    public ResultColumn(ResultSet resultSet, TParseTreeNode columnObject) {
        if (columnObject == null || resultSet == null) {
            throw new IllegalArgumentException("ResultColumn arguments can't be null.");
        }

        id = ++ModelBindingManager.get().TABLE_COLUMN_ID;

        this.resultSet = resultSet;
        resultSet.addColumn(this);

        this.columnObject = columnObject;

        TSourceToken startToken = columnObject.getStartToken();
        TSourceToken endToken = columnObject.getEndToken();

        if (columnObject instanceof TObjectName) {
            if (((TObjectName) columnObject).getColumnNameOnly() != null
                    && !"".equals(((TObjectName) columnObject).getColumnNameOnly())) {
                this.name = ((TObjectName) columnObject).getColumnNameOnly();
            } else {
                this.name = ((TObjectName) columnObject).toString();
            }
        } else {
            this.name = columnObject.toString();
        }

        this.name = SQLUtil.trimColumnStringQuote(name);

        this.fullName = columnObject.toString();

        this.startPosition = new Pair<Long, Long>(startToken.lineNo,
                startToken.columnNo);
        this.endPosition = new Pair<Long, Long>(endToken.lineNo,
                endToken.columnNo + endToken.astext.length());
    }

    public ResultColumn(ResultSet resultSet, TResultColumn resultColumnObject) {
        if (resultColumnObject == null || resultSet == null) {
            throw new IllegalArgumentException("ResultColumn arguments can't be null.");
        }

        id = ++ModelBindingManager.get().TABLE_COLUMN_ID;

        this.resultSet = resultSet;
        resultSet.addColumn(this);

        this.columnObject = resultColumnObject;

        if (resultColumnObject.getExpr().getExpressionType() == EExpressionType.function_t) {
            this.isFunction = true;
        }

        if (resultColumnObject.getAliasClause() != null) {
            this.alias = resultColumnObject.getAliasClause().toString();
            TSourceToken startToken = resultColumnObject.getAliasClause()
                    .getStartToken();
            TSourceToken endToken = resultColumnObject.getAliasClause()
                    .getEndToken();
            this.aliasStartPosition = new Pair<Long, Long>(startToken.lineNo,
                    startToken.columnNo);
            this.aliasEndPosition = new Pair<Long, Long>(endToken.lineNo,
                    endToken.columnNo + endToken.astext.length());

            this.name = this.alias;
        } else {
            if (resultColumnObject.getExpr().getExpressionType() == EExpressionType.simple_constant_t
                    || resultColumnObject.getExpr().getExpressionType() == EExpressionType.function_t
                    || isExpressionArthematic(resultColumnObject.getExpr().getExpressionType())) {

                if (resultSet instanceof SelectResultSet) {//Fix for Expression coming as columnName
                    this.name = "DUMMY"
                            + getIndexOf(((SelectResultSet) resultSet).getResultColumnObject(),
                                    resultColumnObject);
                } else if (resultSet instanceof SelectSetResultSet) {
                    this.name = "DUMMY"
                            + getIndexOf(((SelectSetResultSet) resultSet).getResultColumnObject(),
                                    resultColumnObject);
                } else {
                    this.name = resultColumnObject.toString();
                }

            } else if (resultColumnObject.getExpr().getExpressionType() == EExpressionType.sqlserver_proprietary_column_alias_t) {
                this.alias = resultColumnObject.getExpr()
                        .getLeftOperand()
                        .toString();
                TSourceToken startToken = resultColumnObject.getExpr()
                        .getLeftOperand()
                        .getStartToken();
                TSourceToken endToken = resultColumnObject.getExpr()
                        .getLeftOperand()
                        .getEndToken();
                this.aliasStartPosition = new Pair<Long, Long>(startToken.lineNo,
                        startToken.columnNo);
                this.aliasEndPosition = new Pair<Long, Long>(endToken.lineNo,
                        endToken.columnNo + endToken.astext.length());

                this.name = this.alias;
            } else if (resultColumnObject.getExpr().getExpressionType() == EExpressionType.function_t) {
                this.name = resultColumnObject.getExpr()
                        .getFunctionCall()
                        .getFunctionName()
                        .toString();
                this.fullName = resultColumnObject.getExpr()
                        .getFunctionCall()
                        .getFunctionName()
                        .toString();
            } else if (resultColumnObject.getColumnNameOnly() != null
                    && !"".equals(resultColumnObject.getColumnNameOnly())) {
                this.name = resultColumnObject.getColumnNameOnly();
            } else {
                this.name = resultColumnObject.toString();
                this.fullName = resultColumnObject.toString();
            }
        }

        this.name = SQLUtil.trimColumnStringQuote(name);

        TSourceToken startToken = resultColumnObject.getStartToken();
        TSourceToken endToken = resultColumnObject.getEndToken();
        this.startPosition = new Pair<Long, Long>(startToken.lineNo,
                startToken.columnNo);
        this.endPosition = new Pair<Long, Long>(endToken.lineNo,
                endToken.columnNo + endToken.astext.length());
    }

    public ResultColumn(SelectResultSet resultSet,
            Pair<TResultColumn, TObjectName> starColumnPair) {
        if (starColumnPair == null || resultSet == null) {
            throw new IllegalArgumentException("ResultColumn arguments can't be null.");
        }

        id = ++ModelBindingManager.get().TABLE_COLUMN_ID;

        this.resultSet = resultSet;
        resultSet.addColumn(this);

        if (starColumnPair.first != null && starColumnPair.first.getExpr().getExpressionType() == EExpressionType.function_t) {
            this.isFunction = true;
        }

        this.columnObject = starColumnPair.first;

        TSourceToken startToken = columnObject.getStartToken();
        TSourceToken endToken = columnObject.getEndToken();

        this.name = ((TObjectName) columnObject).getColumnNameOnly();
        this.fullName = columnObject.toString();

        this.name = SQLUtil.trimColumnStringQuote(name);

        this.startPosition = new Pair<Long, Long>(startToken.lineNo,
                startToken.columnNo);
        this.endPosition = new Pair<Long, Long>(endToken.lineNo,
                endToken.columnNo + endToken.astext.length());
    }

    private int getIndexOf(TResultColumnList resultColumnList,
            TResultColumn resultColumnObject) {
        for (int i = 0; i < resultColumnList.size(); i++) {
            if (resultColumnList.getResultColumn(i) == resultColumnObject) {
                return i;
            }
        }
        return -1;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public int getId() {
        return id;
    }

    public String getAlias() {
        return alias;
    }

    public Pair<Long, Long> getAliasStartPosition() {
        return aliasStartPosition;
    }

    public Pair<Long, Long> getAliasEndPosition() {
        return aliasEndPosition;
    }

    public String getFullName() {
        return fullName;
    }

    public Pair<Long, Long> getStartPosition() {
        return startPosition;
    }

    public Pair<Long, Long> getEndPosition() {
        return endPosition;
    }

    public TParseTreeNode getColumnObject() {
        return columnObject;
    }

    public String getName() {
        return name;
    }

    public void bindStarLinkColumn(TObjectName objectName) {
        if (objectName != null && !starLinkColumns.contains(objectName)) {
            if ("*".equals(getColumnName(objectName))) {
                return;
            }
            starLinkColumns.add(objectName);
        }
    }

    private String getColumnName(TObjectName column) {
        if (column == null) {
            return null;
        }
        String name = column.getColumnNameOnly();
        if (name == null || "".equals(name.trim())) {
            return SQLUtil.trimColumnStringQuote(column.toString().trim());
        } else {
            return SQLUtil.trimColumnStringQuote(name.trim());
        }
    }

    public List<TObjectName> getStarLinkColumns() {
        return starLinkColumns;
    }

    public boolean isShowStar() {
        return showStar;
    }

    public void setShowStar(boolean showStar) {
        this.showStar = showStar;
    }

    public boolean isFunction() {
        return isFunction;
    }

    public void setFunction(boolean isFunction) {
        this.isFunction = isFunction;
    }

    //Fix For New Method
    private boolean isExpressionArthematic(EExpressionType expressionType) {
        boolean flag = false;
        try {
            if (expressionType == EExpressionType.arithmetic_compound_operator_t
                    || expressionType == EExpressionType.arithmetic_divide_t
                    || expressionType == EExpressionType.arithmetic_exponentiation_t
                    || expressionType == EExpressionType.arithmetic_minus_t
                    || expressionType == EExpressionType.arithmetic_modulo_t
                    || expressionType == EExpressionType.arithmetic_plus_t
                    || expressionType == EExpressionType.arithmetic_t
                    || expressionType == EExpressionType.arithmetic_times_t) {
                flag = true;
            }
        } catch (Exception e) {
            System.out.println("Exception in com.erwin.dataflow.model.ResultColumn.isExpressionIsArthematic():" + e.toString());
        }
        return flag;

    }
}

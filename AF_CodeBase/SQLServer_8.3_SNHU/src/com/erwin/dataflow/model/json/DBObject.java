package com.erwin.dataflow.model.json;

import com.alibaba.fastjson.annotation.JSONField;

public class DBObject {
	@JSONField(ordinal = 1)
	private String id;
	@JSONField(ordinal = 2)
	private String database;
	@JSONField(ordinal = 3)
	private String schema;
	@JSONField(ordinal = 4)
	private String name;
	@JSONField(ordinal = 5)
	private String alias;
	@JSONField(ordinal = 6)
	private String type;
	@JSONField(ordinal = 7)
	private String tableType;
	@JSONField(ordinal = 8)
	private Argument[] arguments;
	@JSONField(ordinal = 9)
	private Column[] columns;
	@JSONField(ordinal = 10)
	private Coordinate[] coordinates;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Argument[] getArguments() {
		return arguments;
	}

	public void setArguments(Argument[] arguments) {
		this.arguments = arguments;
	}

	public Column[] getColumns() {
		return columns;
	}

	public void setColumns(Column[] columns) {
		this.columns = columns;
	}

	public Coordinate[] getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(Coordinate[] coordinates) {
		this.coordinates = coordinates;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}
	
}

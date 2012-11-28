package com.hbase.dataStructs;

public class TableConf {

	private String name;
	private String family;
	private String fields;
	private String fieldTypes;

	public void setConfiguration(Configuration config) {

		this.setName(config.getTableName());
		this.setFamily(config.getColumnFamily());
		this.setFields(config.getColumnFields());
		this.setFieldTypes(config.getColumnTypes());
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	private void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the family
	 */
	public String getFamily() {
		return family;
	}

	/**
	 * @param family
	 *            the family to set
	 */
	private void setFamily(String family) {
		this.family = family;
	}

	/**
	 * @return the fields
	 */
	public String getFields() {
		return fields;
	}

	/**
	 * @param fields
	 *            the fields to set
	 */
	private void setFields(String fields) {
		this.fields = fields;
	}

	/**
	 * @return the fieldTypes
	 */
	public String getFieldTypes() {
		return fieldTypes;
	}

	/**
	 * @param fieldTypes
	 *            the fieldTypes to set
	 */
	private void setFieldTypes(String fieldTypes) {
		this.fieldTypes = fieldTypes;
	}
}

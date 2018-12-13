package com.baosight.xinsight.ots.rest.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.utils.JsonUtil;


/**
 * Simple representation of a list of table names.
 */
@XmlRootElement(name="tables")
@XmlAccessorType(XmlAccessType.FIELD)
public class TableListModel implements Serializable {

	@JsonIgnore
	private static final long serialVersionUID = 1L;

	@JsonProperty(value="errcode")
	private Long errcode;
		
	@JsonProperty(value="total_count")
	private Long totalcount;

	@JsonProperty(value="table_names")
	private List<String> tables = new ArrayList<String>();

	/**
	 * Default constructor
	 */
	public TableListModel() {}

	public TableListModel(Long totalcount) {
		super();
		this.totalcount = totalcount;
		this.tables.clear();
	}
	
	public TableListModel(Long totalcount, List<String> tables) {
		super();
		this.totalcount = totalcount;
		this.tables = tables;
	}

	@XmlElement
	public Long getTotalcount() {
		return totalcount;
	}

	public void setTotalcount(Long totalcount) {
		this.totalcount = totalcount;
	}
	
	@XmlElement
	public Long getErrcode() {
		return errcode;
	}

	public void setErrcode(Long errcode) {
		this.errcode = errcode;
	}
	
	/**
	 * Add the table name model to the list
	 * @param table the table model
	 */
	public void add(String table) {
		tables.add(table);
	}
	
	/**
	 * @param index the index
	 * @return the table model
	 */
	public String get(int index) {
		return tables.get(index);
	}

	/**
	 * @return the tables
	 */
	@XmlElement
	public List<String> getTables() {
		return tables;
	}

	/**
	 * @param tables the tables to set
	 */
	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	@JsonIgnore
	@XmlTransient
	@Override
	public String toString() {
		return JsonUtil.toJsonString(this);
	}
}

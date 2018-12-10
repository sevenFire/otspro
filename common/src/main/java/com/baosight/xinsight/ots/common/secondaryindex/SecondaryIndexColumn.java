package com.baosight.xinsight.ots.common.secondaryindex;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.exception.OtsException;

@XmlRootElement(name = "column")
@XmlAccessorType(XmlAccessType.FIELD)
public class SecondaryIndexColumn implements Serializable {
	@JsonIgnore
	private static final long serialVersionUID = 1L;

	/**
	 * xml/json元素名称同变量名
	 */
	@JsonProperty(value = "name")
	private String name;
	@JsonProperty(value = "type")
	private ValueType type;
	@JsonProperty(value = "maxLen")
	private Integer maxLen = 0;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ValueType getType() {
		return type;
	}

	public void setType(ValueType type) {
		this.type = type;
	}

	public void setMaxLen(Integer maxLen) {
		this.maxLen = maxLen;
	}

	public enum ValueType {
		int32, int64, float32, float64, string, binary
	};

	public SecondaryIndexColumn() {
	}

	public SecondaryIndexColumn(String colName) {
		name = colName;
		this.type = ValueType.string;
		this.maxLen = OtsConstants.OTS_SEC_INDEX_DEF_COL_MAX_LEN;
	}

	public SecondaryIndexColumn(String colName, ValueType type) {
		name = colName;
		this.type = type;
	}

	public SecondaryIndexColumn(String colName, ValueType type, Integer maxLen) throws OtsException {
		this.name = colName;
		this.type = type;
		if (type == ValueType.string || type == ValueType.binary) {
			if (maxLen == null || maxLen <= 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_MAX_LEN, "Invalid type maxLen");
			}
		}

		this.maxLen = maxLen;

	}

	public int getMaxLen() throws OtsException {
		switch (type) {
		case string:
		case binary:
			if (maxLen == null || maxLen <= 0) {
				throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_MAX_LEN, "Invalid type maxLen");
			}
			return maxLen;
		case int32:
			return OtsConstants.OTS_SEC_INDEX_TYPE_INT_LEN;
		case float32:
			return OtsConstants.OTS_SEC_INDEX_TYPE_FLOAT_LEN;
		case int64:
			return OtsConstants.OTS_SEC_INDEX_TYPE_LONG_LEN;
		case float64:
			return OtsConstants.OTS_SEC_INDEX_TYPE_DOUBLE_LEN;
		default:
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_MAX_LEN, "Invalid type maxLen");
		}
	}

	public String toString() {
		if (type == ValueType.string || type == ValueType.binary)
			return name + "->" + type + "&" + this.maxLen;
		else
			return name + "->" + type;
	}
}

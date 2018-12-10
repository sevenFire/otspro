package com.baosight.xinsight.ots.client.secondaryindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn.ValueType;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexInfo;
import com.baosight.xinsight.ots.exception.OtsException;

public class SecondIndexQueryOption {
	private static final String SEC_INDEX_QUERY_START = "*";
	private static final String SEC_INDEX_QUERY_END = "**";
	
	private SecondaryIndexInfo index;
	List<byte[]> columns = new ArrayList<byte[]>();
	byte[] startUserTableKey;
	byte[] stopUserTableKey;
	private String columnRanges;
	String cursor = SEC_INDEX_QUERY_START;

	private int limit = OtsConstants.DEFAULT_QUERY_LIMIT;
	private int offset = OtsConstants.DEFAULT_QUERY_OFFSET;

	private Map<String, ColumnRange> ranges = new HashMap<String, ColumnRange>();

	public void updateCursor(byte[] nextRowKey) {
		this.cursor = Hex.encodeHexString(nextRowKey);
	}

	public boolean isEnd() {
		return cursor.equals(SEC_INDEX_QUERY_END);
	}

	public void setEnd() {
		cursor = SEC_INDEX_QUERY_END;
	}

	public void setCursor(String cursor) {
		if (cursor != null) {
			this.cursor = cursor;
		}
	}

	public String getCursor() {
		return cursor;
	}

	public void setIndex(SecondaryIndexInfo index)	throws OtsException {
		this.index = index;
		if (null != columnRanges) {
			parseColumnRanges(columnRanges);
		}
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) throws OtsException {
		if (limit < 0)
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_VALUE,	"Invalid limit value!");
		this.limit = limit;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) throws OtsException {
		if (offset < 0)
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_VALUE,	"Invalid offset value!");
		this.offset = offset;
	}

	public boolean hasColumns() {
		if (columns == null || columns.isEmpty()) {
			return false;
		}
		return true;
	}

	public boolean onlyGetRowKey() {
//		if (hasColumns()) {
//			if (columns.size() == 1 && columns.get(0).equals(OtsConstants.DEFAULT_ROWKEY_NAME)) {
//				return true;
//			}
//		}
		
		return false;
	}

	public byte[] getStartUserTableKey() {
		return startUserTableKey;
	}

	public void setStartUserTableKey(byte[] startUserTableKey) {
		this.startUserTableKey = startUserTableKey;
	}

	public byte[] getStopUserTableKey() {
		return stopUserTableKey;
	}

	public void setStopUserTableKey(byte[] stopUserTableKey) {
		this.stopUserTableKey = stopUserTableKey;
	}

	public List<byte[]> getColumns() {
		return columns;
	}

	public Map<String, ColumnRange> getRanges() {
		return ranges;
	}

	public SecondIndexQueryOption(SecondaryIndexInfo index) {
		this.index = index;
	}

	public SecondIndexQueryOption() {
	}

	public class ColumnRange {
		private ValueType type;
		private String name;
		private byte[] start;
		private byte[] stop;

		public ColumnRange(String name, int start, int stop) {
			this.setName(name);
			this.start = Bytes.toBytes(start);
			this.stop = Bytes.toBytes(stop);
			this.setType(ValueType.int32);
		}

		public ColumnRange(String name, String start, String stop) {
			this.setName(name);
			this.start = start == null ? null : Bytes.toBytes(start);
			this.stop = stop == null ? null : Bytes.toBytes(stop);
			this.setType(ValueType.string);
		}

		public byte[] getStart() {
			return start;
		}

		public byte[] getStop() {
			return stop;
		}

		public ColumnRange(String name, Float start, Float stop) {
			this.setName(name);
			this.start = start == null ? null : Bytes.toBytes(start);
			this.stop = stop == null ? null : Bytes.toBytes(stop);
			this.setType(ValueType.float32);
		}

		public ColumnRange(String name, Long start, Long stop) {
			this.setName(name);
			this.start = start == null ? null : Bytes.toBytes(start);
			this.stop = stop == null ? null : Bytes.toBytes(stop);
			this.setType(ValueType.int64);
		}

		public ColumnRange(String name, Double start, Double stop) {
			this.setName(name);
			this.start = start == null ? null : Bytes.toBytes(start);
			this.stop = stop == null ? null : Bytes.toBytes(stop);
			this.setType(ValueType.float64);
		}

		public ColumnRange(String name, byte[] start, byte[] stop) {
			this.setName(name);
			this.start = start == null ? null : start;
			this.stop = stop == null ? null : stop;
			this.setType(ValueType.binary);
		}

		public ValueType getType() {
			return type;
		}

		public void setType(ValueType type) {
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	public void addColumnRange(String name, int start, int stop) throws OtsException {
		if (index.existColumn(name)) {
			ranges.put(name, new ColumnRange(name, start, stop));
		} else {
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_NO_EXIST,
					String.format("SecondaryIndex column no exist, name:%s, type:%s", name, ValueType.int32));
		}
	}

	public void addColumnRange(String name, String start, String stop) throws OtsException {
		if (name != null) {
			name = name.trim();
		}

		if (!index.existColumn(name)) {
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_NO_EXIST,
					String.format("SecondaryIndex column no exist, name:%s", name));
		}

		ValueType type = index.getColumnType(name);
		if (start != null) {
			start = start.trim();
			if (start.length() == 0 || start.equals("*"))
				start = null;
		}

		if (stop != null) {
			stop = stop.trim();
			if (stop.length() == 0 || stop.equals("*"))
				stop = null;
		}

		switch (type) {
		case string:
			ranges.put(name, new ColumnRange(name, start, stop));
			break;
		case int32:
			addColumnRange(name, start == null ? null : Integer.parseInt(start),
					stop == null ? null : Integer.parseInt(stop));
			break;
		case float32:
			addColumnRange(name, start == null ? null : Float.parseFloat(start),
					stop == null ? null : Float.parseFloat(stop));
			break;
		case int64:
			addColumnRange(name, start == null ? null : Long.parseLong(start),
					stop == null ? null : Long.parseLong(stop));
			break;
		case float64:
			addColumnRange(name, start == null ? null : Double.parseDouble(start),
					stop == null ? null : Double.parseDouble(stop));
			break;
		case binary:
			addColumnRange(name, start == null ? null : Bytes.toBytes(start),
					stop == null ? null : Bytes.toBytes(stop));
			break;
		default:
			throw new OtsException(
					OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_INVALID_TYPE,
					String.format(
							"SecondaryIndex column type invalid, type:%d", type));
		}

	}

	public void addColumnRange(String name, Float start, Float stop) throws OtsException {
		if (index.existColumn(name)) {
			ranges.put(name, new ColumnRange(name, start, stop));
		} else {
			throw new OtsException(
					OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_NO_EXIST,
					String.format("SecondaryIndex column no exist, name:%s, type:%s", name, ValueType.float32));
		}
	}

	public void addColumnRange(String name, Long start, Long stop) throws OtsException {
		if (index.existColumn(name)) {
			ranges.put(name, new ColumnRange(name, start, stop));
		} else {
			throw new OtsException(
					OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_NO_EXIST,
					String.format("SecondaryIndex column no exist, name:%s, type:%s", name, ValueType.int64));
		}
	}

	public void addColumnRange(String name, Double start, Double stop)
			throws OtsException {
		if (index.existColumn(name)) {
			ranges.put(name, new ColumnRange(name, start, stop));
		} else {
			throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_NO_EXIST,
					String.format("SecondaryIndex column no exist, name:%s, type:%s", name, ValueType.float64));
		}
	}

	public void addColumnRange(String name, byte[] start, byte[] stop) throws OtsException {
		if (index.existColumn(name)) {
			ranges.put(name, new ColumnRange(name, start, stop));
		} else {
			throw new OtsException(
					OtsErrorCode.EC_OTS_SEC_INDEX_COLUMN_NO_EXIST,
					String.format("SecondaryIndex column no exist, name:%s, type:%s", name, ValueType.binary));
		}
	}

	public void addColumn(byte[] column) {
		this.columns.add(column);
	}

	public void addColumn(String column) {
		this.columns.add(Bytes.toBytes(column));
	}

	public void deleteColumnRange(String name) {
		ranges.remove(name.trim());
	}

	public byte[] getStartRowKey() throws DecoderException, OtsException {
		if (cursor.equals(SEC_INDEX_QUERY_START)) {
			List<SecondaryIndexColumn> columns = index.getColumns();
			int keyLen = index.getKeyLength();
			byte[] key = null;
			int offset = 0;
			int curColumnMaxOffset = 0;

			if (null == startUserTableKey) {
				key = new byte[keyLen];
			} else {
				key = new byte[keyLen + startUserTableKey.length];
			}

			for (SecondaryIndexColumn column : columns) {
				curColumnMaxOffset = offset + column.getMaxLen();
				ColumnRange columnRange = ranges.get(column.getName());
				if (columnRange != null) {
					byte[] startColumnRange = columnRange.getStart();
					if (startColumnRange != null) {
						if (startColumnRange.length > column.getMaxLen()) {
							System.arraycopy(startColumnRange, 0, key, offset, column.getMaxLen());
							offset += column.getMaxLen();
						} else {
							System.arraycopy(startColumnRange, 0, key, offset, startColumnRange.length);
							offset += startColumnRange.length;
						}
					}
				}

				for (int j = offset; j < curColumnMaxOffset; ++j) {
					key[j] = 0x00;
					offset++;
				}
			}

			if (null != startUserTableKey) {
				System.arraycopy(startUserTableKey, 0, key, offset, startUserTableKey.length);
			}
			return key;
			
		} else {
			return Hex.decodeHex(cursor.toCharArray());
		}
	}

	public byte[] getStopRowKey() throws OtsException {
		List<SecondaryIndexColumn> columns = index.getColumns();
		int keyLen = index.getKeyLength();
		byte[] key = null;
		int offset = 0;
		int curColumnMaxOffset = 0;

		if (null == stopUserTableKey) {
			key = new byte[keyLen];
		} else {
			key = new byte[keyLen + stopUserTableKey.length];
		}

		for (SecondaryIndexColumn column : columns) {
			curColumnMaxOffset = offset + column.getMaxLen();
			ColumnRange columnRange = ranges.get(column.getName());
			if (columnRange != null) {
				byte[] stopColumnRange = columnRange.getStop();
				if (null != stopColumnRange) {
					if (stopColumnRange.length > column.getMaxLen()) {
						System.arraycopy(stopColumnRange, 0, key, offset, column.getMaxLen());
						offset += column.getMaxLen();
					} else {
						System.arraycopy(stopColumnRange, 0, key, offset, stopColumnRange.length);
						offset += stopColumnRange.length;
					}
				}
			}

			for (int j = offset; j < curColumnMaxOffset; ++j) {
				key[j] = (byte) 0xff;
				offset++;
			}
		}

		if (null != stopUserTableKey) {
			System.arraycopy(stopUserTableKey, 0, key, offset, stopUserTableKey.length);
		}

		return key;
	}

	private void parseColumnRanges(String columnRanges) throws OtsException {
		String[] ranges = columnRanges.split(",");
		for (String range : ranges) {
			String[] rangeInfos = range.split(":");
			if (rangeInfos.length < 2)
				continue;

			String trimRange = rangeInfos[1].replace('[', ' ')
					.replace(']', ' ').trim();
			Pattern pattern = Pattern.compile("(\\sTO\\s)");
			String[] rangeValue = pattern.split(trimRange);
			if (rangeValue.length < 2) {
				throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_COLUMN_RANGE, "Invalid column range");
			}
			addColumnRange(rangeInfos[0], rangeValue[0], rangeValue[1]);
		}
	}

	public void setColumnRanges(String columnRanges) throws OtsException {
		this.columnRanges = columnRanges;
		if (null != index) {
			parseColumnRanges(this.columnRanges);
		}
	}

	public void setColumns(String columns) {
		if (columns != null) {
			String[] columnNames = columns.split(",");
			for (String column : columnNames) {
				addColumn(column);
			}
		}
	}
}

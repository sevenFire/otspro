package com.baosight.xinsight.ots.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexInfo;
import com.baosight.xinsight.ots.common.util.SecondaryIndexUtil;
import com.baosight.xinsight.ots.exception.OtsException;

public class IndexRegionObserver extends BaseRegionObserver implements RegionObserver {

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class);

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {

	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {

	}

	private byte[] generateIndexRowKey(Put put, SecondaryIndexInfo indexSpec) throws OtsException {
		int indexKeyLen = indexSpec.getKeyLength();
		List<SecondaryIndexColumn> columns = indexSpec.getColumns();
		byte[] oldKey = put.getRow();
		byte[] newKey = new byte[indexKeyLen + oldKey.length];
		byte[] realValue = null;

		int offset = 0;
		for (SecondaryIndexColumn column : columns) {
			List<Cell> cells = put.get(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME), Bytes.toBytes(column.getName()));
			int lack = column.getMaxLen();

			if (cells.size() > 0) {
				byte[] cellValue = CellUtil.cloneValue(cells.get(0));
				switch (column.getType()) {
				case string:
				case binary:
					if (cellValue.length > column.getMaxLen()) {
						System.arraycopy(cellValue, 0, newKey, offset, column.getMaxLen());
						offset += column.getMaxLen();
						lack -= column.getMaxLen();
					} else {
						System.arraycopy(cellValue, 0, newKey, offset, cellValue.length);
						offset += cellValue.length;
						lack -= cellValue.length;
					}
					break;
				case int32:
					realValue = Bytes.toBytes(Integer.parseInt(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				case float32:
					realValue = Bytes.toBytes(Float.parseFloat(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				case float64:
					realValue = Bytes.toBytes(Double.parseDouble(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				case int64:
					realValue = Bytes.toBytes(Long.parseLong(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				default:
					break;
				}
			}

			for (int j = 0; j < lack; ++j) {
				newKey[offset + j] = 0;
			}
			offset += lack;
		}

		System.arraycopy(oldKey, 0, newKey, offset, oldKey.length);
		return newKey;
	}

	private byte[] generateIndexRowKey(Delete del, SecondaryIndexInfo indexSpec) throws OtsException {
		int indexKeyLen = indexSpec.getKeyLength();
		List<SecondaryIndexColumn> columns = indexSpec.getColumns();
		byte[] oldKey = del.getRow();
		byte[] newKey = new byte[indexKeyLen + oldKey.length];
		byte[] realValue = null;

		int offset = 0;
		List<Cell> cells = del.getFamilyCellMap().get(Bytes.toBytes(OtsConstants.DEFAULT_FAMILY_NAME));
		for (SecondaryIndexColumn column : columns) {
			byte[] cellValue = null;
			byte[] curCol = Bytes.toBytes(column.getName());
			for (Cell tmpCell : cells) {
				byte[] col = CellUtil.cloneQualifier(tmpCell);
				if (0 == Bytes.compareTo(col, curCol))
					cellValue = CellUtil.cloneValue(tmpCell);
			}

			int lack = column.getMaxLen();
			if (null != cellValue) {
				switch (column.getType()) {
				case string:
				case binary:
					if (cellValue.length > column.getMaxLen()) {
						System.arraycopy(cellValue, 0, newKey, offset, column.getMaxLen());
						offset += column.getMaxLen();
						lack -= column.getMaxLen();
					} else {
						System.arraycopy(cellValue, 0, newKey, offset, cellValue.length);
						offset += cellValue.length;
						lack -= cellValue.length;
					}
					break;
				case int32:
					realValue = Bytes.toBytes(Integer.parseInt(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				case float32:
					realValue = Bytes.toBytes(Float.parseFloat(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				case float64:
					realValue = Bytes.toBytes(Double.parseDouble(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				case int64:
					realValue = Bytes.toBytes(Long.parseLong(Bytes.toString(cellValue)));
					System.arraycopy(realValue, 0, newKey, offset, column.getMaxLen());
					offset += column.getMaxLen();
					lack -= column.getMaxLen();
					break;
				default:
					break;
				}
			}

			for (int j = 0; j < lack; ++j) {
				newKey[offset + j] = 0;
			}
			offset += lack;
		}

		System.arraycopy(oldKey, 0, newKey, offset, oldKey.length);
		return newKey;
	}

	@Override
	public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp)	throws IOException {
		HTableDescriptor htd = c.getEnvironment().getRegion().getTableDesc();
		try {
			List<SecondaryIndexInfo> indexes = SecondaryIndexUtil.parseIndexes(htd.getValue(OtsConstants.OTS_INDEXES));
			for (SecondaryIndexInfo index : indexes) {
				String indexTableName = SecondaryIndexUtil.getIndexTableName(htd.getTableName().getNameAsString(), index.getName());
				
				Table table = null;
				try{
					table = c.getEnvironment().getTable(TableName.valueOf(indexTableName));
					List<Put> puts = new ArrayList<Put>();
					List<Delete> dels = new ArrayList<Delete>();
					for (int i = 0; i < miniBatchOp.size(); ++i) {
						Mutation opt = miniBatchOp.getOperation(i);
						if (opt instanceof Put) {
							Put newRow = new Put(generateIndexRowKey((Put) opt,	index));
							newRow.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("c"));
							puts.add(newRow);
						} else if (opt instanceof Delete) {
							Delete newDel = new Delete(generateIndexRowKey((Delete)opt, index));
							dels.add(newDel);
						}
					}
	
					if (puts.size() > 0) {
						table.put(puts);
						puts.clear();
					}
	
					if (dels.size() > 0) {
						table.delete(dels);
						dels.clear();
					}
				}finally{
					if (null != table)
						table.close();
				}
			}
		} catch (NumberFormatException e) {
			throw new IOException(e.getMessage());
		} catch (OtsException e) {
			throw new IOException(e.getErrorCode() + " " + e.getMessage());
		}
	}
}

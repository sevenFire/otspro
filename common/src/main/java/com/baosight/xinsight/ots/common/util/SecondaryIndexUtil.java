package com.baosight.xinsight.ots.common.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;

import com.baosight.xinsight.ots.OtsConstants;
import com.baosight.xinsight.ots.OtsErrorCode;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexColumn.ValueType;
import com.baosight.xinsight.ots.common.secondaryindex.SecondaryIndexInfo;
import com.baosight.xinsight.ots.exception.OtsException;
import com.baosight.xinsight.utils.BytesUtil;
import com.baosight.xinsight.yarn.YarnAppUtil;
import com.baosight.xinsight.yarn.YarnTagGenerator;

public class SecondaryIndexUtil {
	/**
	 * Utility method to get the name of the index table when given the name of
	 * the actual table.
	 * 
	 * @param tableName
	 * @return index table name
	 */
	public static String getIndexTableName(String tableName, String indexName) {
		return tableName + OtsConstants.OTS_INDEX_TABLE_SPLIT + indexName + OtsConstants.OTS_INDEX_TABLE_SUFFIX;
	}

	public static byte[] concat(byte[] first, byte[]... rest) {
		int totalLength = first.length;
		for (byte[] array : rest) {
			totalLength += array.length;
		}
		byte[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (byte[] array : rest) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}

	public static byte[] getIndexTableName(byte[] tableName, byte[] indexName) {
		// The suffix for the index table is fixed now. Do we allow to make this
		// configurable?
		// We can handle things in byte[] way?
		return concat(tableName, BytesUtil.toBytes(OtsConstants.OTS_INDEX_TABLE_SPLIT), indexName, 
				BytesUtil.toBytes(OtsConstants.OTS_INDEX_TABLE_SUFFIX));
	}

	public static boolean cotainIndex(List<SecondaryIndexInfo> indexes, String indexName) {
		for (SecondaryIndexInfo index : indexes) {
			if (index.getName().equals(indexName))
				return true;
		}
		return false;
	}

	public static List<SecondaryIndexInfo> parseIndexes(String otsIndexes) throws OtsException {
		List<SecondaryIndexInfo> indexes = new ArrayList<SecondaryIndexInfo>();
		if (null != otsIndexes && otsIndexes.length() > 0) {
			String[] indexSplits = otsIndexes.split("#");
			for (String index : indexSplits) {
				String[] indexName = index.split("=>");
				if (indexName.length < 2) {
					throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_INDEX_INFO, 
							"invalid ots secondary index info: " + otsIndexes);
				}
				SecondaryIndexInfo indexInfo = new SecondaryIndexInfo(indexName[0]);

				String[] cSplits = indexName[1].split(",");
				if (cSplits.length < 1) {
					throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_INDEX_INFO, 
							"invalid ots secondary index info: " + otsIndexes);
				} else {
					for (String c : cSplits) {
						String[] qualSplits = c.split("->");
						if (qualSplits.length < 2) {
							indexInfo.addColumn(qualSplits[0]);
						} else {
							String[] typeSplits = qualSplits[1].split("&");
							if (typeSplits.length < 2) {
								indexInfo.addColumn(qualSplits[0], ValueType.valueOf(qualSplits[1]));
							} else {
								try {
									indexInfo.addColumn(qualSplits[0], ValueType.valueOf(typeSplits[0]), Integer.parseInt(typeSplits[1]));
								} catch (NumberFormatException e) {
									throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_TYPE_LEN, 
											"Failed to add column, invalid column length: " + typeSplits[1]);
								} catch (IOException e) {
									throw new OtsException(OtsErrorCode.EC_OTS_SEC_INDEX_INVALID_COLUMN_TYPE, 
											"Failed to add column, invalid column type" + typeSplits[0]);
								}
							}
						}
					}
				}
				indexes.add(indexInfo);
			}
		}

		return indexes;
	}

	public static SecondaryIndexInfo constructIndex(String indexName, String columns) throws OtsException {
		SecondaryIndexInfo index = new SecondaryIndexInfo(indexName);
		String[] indexColumns = columns.split(",");
		for (String indexColumn : indexColumns) {
			String[] name = indexColumn.split("->");
			index.addColumn(name[0], ValueType.valueOf(name[1]));
		}

		return index;
	}

	public static String indexesToString(List<SecondaryIndexInfo> indexes) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < indexes.size(); ++i) {
			SecondaryIndexInfo index = indexes.get(i);
			sb.append(index.getName());
			sb.append("=>");
			List<SecondaryIndexColumn> columns = index.getColumns();
			Iterator<SecondaryIndexColumn> iter = columns.iterator();
			while (iter.hasNext()) {
				sb.append(iter.next().toString());
				if (iter.hasNext())
					sb.append(",");
			}

			if (i + 1 < indexes.size())
				sb.append("#");
		}

		return sb.toString();
	}

	public static boolean isIndexBuilding(YarnAppUtil yarnAppUtil, long tid, String tablename, long iid, String indexname) 
			throws ClientProtocolException, IOException {
		Map<String, Object> appStates = yarnAppUtil.getAppStateByAppTag(YarnTagGenerator.GenSecIndexBuildMapreduceTag(tid, 
				tablename, iid, indexname));
		String appState = appStates.get(YarnAppUtil.XINSIGHT_YARN_APP_INFO_STATE).toString();
		String isUncertain = appStates.get(YarnAppUtil.XINSIGHT_YARN_APP_INFO_UNCERTAIN).toString();
		if (isUncertain.equals("true")
				|| appState.equals(YarnAppUtil.YARN_APP_STATE_FINISHED)
				|| appState.equals(YarnAppUtil.YARN_APP_STATE_FAILED)
				|| appState.equals(YarnAppUtil.YARN_APP_STATE_KILLED)) {
			return false;
		} else {
			return true;
		}
	}
}

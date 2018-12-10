package com.baosight.xinsight.ots.common.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;

import com.baosight.xinsight.utils.BytesUtil;

public class PrimaryKeyUtil {
	final static int INTEGER_BYTES_LENGTH = 4;
	//final static int MD5_BYTES_LENGTH = 16;
	
	final static int MD5_BYTES_LENGTH_SHORT = 4;
	
	public final static int TYPE_STRING = 0;
	public final static int TYPE_NUMBER = 1;
	public final static int TYPE_BINARY = 2;
	
	public static final int ROWKEY_TYPE_RANGE = 1;
	public static final int ROWKEY_TYPE_HASH = 0;
	
	/**
	 * @param hashKey, TYPE_STRING default
	 * @return primaryKey
	 */
	public static byte[] buildPrimaryKey(byte[] hashKey) {
		MessageDigest md5Digest;
		try {
			md5Digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
		md5Digest.update(hashKey);
		
		byte[] digest = md5Digest.digest();
		byte[] hashKeyLenByte = BytesUtil.toBytes(hashKey.length);
		byte[] primaryKey = new byte[MD5_BYTES_LENGTH_SHORT + hashKeyLenByte.length + hashKey.length];

		BytesUtil.putBytes(primaryKey, 0, digest, 0, MD5_BYTES_LENGTH_SHORT);
		BytesUtil.putBytes(primaryKey, MD5_BYTES_LENGTH_SHORT, hashKeyLenByte, 0, hashKeyLenByte.length);
		BytesUtil.putBytes(primaryKey, MD5_BYTES_LENGTH_SHORT + hashKeyLenByte.length, hashKey, 0, hashKey.length);
		return primaryKey;
	}
	
	/**
	 * @param hashKey, TYPE_STRING default
	 * @param rangeKey, TYPE_STRING default
	 * @return primaryKey
	 */
	public static byte[] buildPrimaryKey(byte[] hashKey, byte[] rangeKey) {
		MessageDigest md5Digest;
		try {
			md5Digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
		md5Digest.update(hashKey);

		byte[] digest = md5Digest.digest();
		byte[] hashKeyLenByte = BytesUtil.toBytes(hashKey.length);
		byte[] primaryKey = new byte[MD5_BYTES_LENGTH_SHORT + hashKeyLenByte.length + hashKey.length + rangeKey.length];

		BytesUtil.putBytes(primaryKey, 0, digest, 0, MD5_BYTES_LENGTH_SHORT);
		BytesUtil.putBytes(primaryKey, MD5_BYTES_LENGTH_SHORT, hashKeyLenByte, 0, hashKeyLenByte.length);
		BytesUtil.putBytes(primaryKey, MD5_BYTES_LENGTH_SHORT + hashKeyLenByte.length, hashKey, 0, hashKey.length);
		BytesUtil.putBytes(primaryKey, MD5_BYTES_LENGTH_SHORT + hashKeyLenByte.length + hashKey.length, rangeKey, 0, rangeKey.length);
		return primaryKey;
	}
	
	/**
	 * @param primaryKey
	 * @return hashKey, TYPE_STRING default
	 */
	public static byte[] getHashKey(byte[] primaryKey) {
		byte[] hashKeyLenByte = BytesUtil.copy(primaryKey, MD5_BYTES_LENGTH_SHORT, INTEGER_BYTES_LENGTH);
		int hashKeyLen = BytesUtil.toInt(hashKeyLenByte);
		return BytesUtil.copy(primaryKey, INTEGER_BYTES_LENGTH + MD5_BYTES_LENGTH_SHORT, hashKeyLen);
	}
	
	/**
	 * @param primaryKey
	 * @return rangeKey, TYPE_STRING default
	 */
	public static byte[] getRangeKey(byte[] primaryKey) {
		byte[] hashKeyLenByte = BytesUtil.copy(primaryKey, MD5_BYTES_LENGTH_SHORT, INTEGER_BYTES_LENGTH);
		int hashKeyLen = BytesUtil.toInt(hashKeyLenByte);
		int hashKeyOffset = INTEGER_BYTES_LENGTH + MD5_BYTES_LENGTH_SHORT + hashKeyLen;
		return BytesUtil.copy(primaryKey, INTEGER_BYTES_LENGTH + MD5_BYTES_LENGTH_SHORT + hashKeyLen, primaryKey.length - hashKeyOffset);
	}
	
	public static byte[] buildPrimaryKey(Object hashKey, int hashKeyType) throws Exception {
		
		byte[] byteHashkey = null;
		if (hashKeyType == TYPE_STRING) {
			String strHashKey = hashKey.toString();
			byteHashkey  = BytesUtil.toBytes(strHashKey);
		} else if (hashKeyType == TYPE_BINARY) {
			byteHashkey = Hex.decodeHex(hashKey.toString().toCharArray());			
		} else if (hashKeyType == TYPE_NUMBER) {
			Long hashkey = Long.parseLong(hashKey.toString());
			byteHashkey = BytesUtil.toBytes(hashkey);
		}			
				
		return buildPrimaryKey(byteHashkey);
	}
	
	public static byte[] buildPrimaryKey(Object hashKey, int hashKeyType, Object rangeKey, int rangeKeyType) throws Exception {
		
		byte[] byteHashkey = null;
		if (hashKeyType == TYPE_STRING) {
			String strHashKey = hashKey.toString();
			byteHashkey  = BytesUtil.toBytes(strHashKey);
		} else if (hashKeyType == TYPE_BINARY) {
			byteHashkey = Hex.decodeHex(hashKey.toString().toCharArray());			
		} else if (hashKeyType == TYPE_NUMBER) {
			Long hashkey = Long.parseLong(hashKey.toString());
			byteHashkey = BytesUtil.toBytes(hashkey);
		}		
		
		byte[] byteRangeKey = null;
		if (rangeKeyType == TYPE_STRING) {
			String strRangeKey = rangeKey.toString();
			byteRangeKey  = BytesUtil.toBytes(strRangeKey);
		} else if (rangeKeyType == TYPE_BINARY) {
			byteRangeKey = Hex.decodeHex(rangeKey.toString().toCharArray());
		} else if (rangeKeyType == TYPE_NUMBER) {
			Long rangekey = Long.parseLong(rangeKey.toString());
			byteRangeKey = BytesUtil.toBytes(rangekey);
		}	
		
		return buildPrimaryKey(byteHashkey, byteRangeKey);
	}
	
	public static void main(String[] args) {
		byte[] primaryKey = PrimaryKeyUtil.buildPrimaryKey("hello".getBytes(), "word".getBytes());
		byte[] hashKey = PrimaryKeyUtil.getHashKey(primaryKey);
		System.out.println(BytesUtil.toString(hashKey));
		byte[] rangeKey = PrimaryKeyUtil.getRangeKey(primaryKey);
		System.out.println(BytesUtil.toString(rangeKey));
	}
}

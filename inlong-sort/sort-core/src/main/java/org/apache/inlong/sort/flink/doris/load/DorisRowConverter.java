package org.apache.inlong.sort.flink.doris.load;

import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.*;
import org.apache.inlong.sort.formats.common.TypeInfo;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Map;
import java.sql.Time;
import java.sql.Timestamp;

public class DorisRowConverter {

	public static void setRow(FormatInfo[] formatInfos,String[] fieldNames, Row row, Map<String,Object> rowDataMap) {
		for (int i = 0; i < row.getArity(); ++i) {
            final FormatInfo formatInfo = formatInfos[i];
            final String fieldName = fieldNames[i];
			rowDataMap.put(fieldName, row.getField(i));
            //setField(fieldName,formatInfo,row.getField(i),rowDataMap);
		}
	}

	//private static void setField(String fieldName,FormatInfo formatInfo,
    //                             Object value,Map<String,Object> rowDataMap) {
	//	TypeInfo typeInfo = formatInfo.getTypeInfo();
	//
	//	if (typeInfo instanceof StringTypeInfo) {
    //        rowDataMap.put(fieldName, (String) value);
	//	} else if (typeInfo instanceof BooleanTypeInfo) {
    //        rowDataMap.put(fieldName, (Boolean) value);
	//	} else if (typeInfo instanceof ByteTypeInfo) {
    //        rowDataMap.put(fieldName, (Byte) value);
	//	} else if (typeInfo instanceof ShortTypeInfo) {
    //        rowDataMap.put(fieldName, (Short) value);
	//	} else if (typeInfo instanceof IntTypeInfo) {
    //        rowDataMap.put(fieldName, (Integer) value);
	//	} else if (typeInfo instanceof LongTypeInfo) {
    //        rowDataMap.put(fieldName, (Long) value);
	//	} else if (typeInfo instanceof FloatTypeInfo) {
    //        rowDataMap.put(fieldName, (Float) value);
	//	} else if (typeInfo instanceof DoubleTypeInfo) {
    //        rowDataMap.put(fieldName, (Double) value);
	//	} else if (typeInfo instanceof DecimalTypeInfo) {
    //        rowDataMap.put(fieldName, (BigDecimal) value);
	//	} else if (typeInfo instanceof TimeTypeInfo) {
    //        rowDataMap.put(fieldName, (Time) value);
	//	} else if (typeInfo instanceof DateTypeInfo) {
    //        rowDataMap.put(fieldName, (Date) value);
	//	} else if (typeInfo instanceof TimestampTypeInfo) {
    //        rowDataMap.put(fieldName, (Timestamp) value);
	//	} else {
	//		throw new IllegalArgumentException("Unsupported TypeInfo " + typeInfo.getClass().getName());
	//	}
	//}
}
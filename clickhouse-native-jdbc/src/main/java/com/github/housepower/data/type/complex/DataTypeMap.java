package com.github.housepower.data.type.complex;

import com.github.housepower.data.DataTypeFactory;
import com.github.housepower.data.IDataType;
import com.github.housepower.data.type.DataTypeInt64;
import com.github.housepower.data.type.DataTypeUInt64;
import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.jdbc.ClickHouseStruct;
import com.github.housepower.misc.SQLLexer;
import com.github.housepower.misc.Validate;
import com.github.housepower.serde.BinaryDeserializer;
import com.github.housepower.serde.BinarySerializer;

import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.util.*;

/**
 * @ClassName: DataTypeMap
 * @Description: TODO
 * @Author: Amitola
 * @Date: 2021/5/20
 **/
public class DataTypeMap implements IDataType {

    private final String name;
    private final IDataType<?, ?> key;
    private final IDataType<?, ?> value;
    private final DataTypeArray nestedType;
    private final DataTypeTuple subNestedType;
    private final ClickHouseArray defaultValue;
    private final DataTypeInt64 offsetIDataType;

    private HashMap<IDataType, IDataType> map;
    public static DataTypeCreator<Object, Object> CREATOR =
            (lexer, serverContext) -> {
                Validate.isTrue(lexer.character() == '(');
                IDataType<?, ?> key = DataTypeFactory.get(lexer, serverContext);
                Validate.isTrue(lexer.character() ==',');
                IDataType<?, ?> value = DataTypeFactory.get(lexer, serverContext);
                Validate.isTrue(lexer.character() == ')');
                return new DataTypeMap("Map(" +key + ", " + value + ")",
                        key, value);
            };

    public DataTypeMap(String name, IDataType<?,?> key, IDataType<?,?> value) throws SQLException {
        this.name = name;
        this.key = key;
        this.value =value;
        IDataType [] tupleInner = new IDataType[] {key, value};
        this.subNestedType = new DataTypeTuple("Tuple(" + key + ", " + value + ")", tupleInner);
        this.nestedType = new DataTypeArray("Array(DataTypeTuple)", subNestedType, new DataTypeInt64());
        this.defaultValue = new ClickHouseArray(subNestedType,
                new Object[]{subNestedType.defaultValue()});
        this.offsetIDataType = new DataTypeInt64();
    }

    /*public static Object typeDefine(DataTypeMap type, Object obj) {
        // convert hashmap into ClickhouseArray[Clickhousedict[]]
        if(obj instanceof HashMap) {
            HashMap obj1 = (HashMap) obj;

        }
        return  type.nestedType.defaultValue();
       *//* String keyType = type.getKeyType();
        String valueType = type.getValueType();
        if (keyType.equals("String")) {
            if (valueType.equals("String")){
                return (HashMap<String, String>) obj;
            }
            if (valueType.equals("Integer")){
                return (HashMap<String, Integer>) obj;
            }
            if (valueType.equals("Array")){
                return (HashMap<String, Array>) obj;
            }
        }
        if (keyType.equals("Integer")) {
            if (valueType.equals("String")){
                return (HashMap<Integer, String>) obj;
            }
            if (valueType.equals("Integer")){
                return (HashMap<Integer, Integer>) obj;
            }
            if (valueType.equals("Array")){
                return (HashMap<Integer, Array>) obj;
            }
        }
        throw new IllegalArgumentException("Clickhouse map type only accept key type to be Integer or String and value type " +
                "to be Integer, String, Array");*//*
    }*/

    public IDataType getElementType() {
        return this.nestedType;
    }

    public String getKeyType () {
        return this.key.name();
    }

    public String getValueType () {
        return this.value.name();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Class javaType() {
        return null;
    }

    @Override
    public int sqlTypeId() {
        return 0;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }


    @Override
    public String serializeText(Object value) throws SQLException {
       return null;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object f : ((ClickHouseArray) data).getArray()) {
            subNestedType.serializeBinary((ClickHouseStruct) f, serializer);
        }
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        offsetIDataType.serializeBinary((long) data.length, serializer);
        nestedType.serializeBinaryBulk((ClickHouseArray[]) data, serializer);
    }

    @Override
    public Object deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '{');
        List<Object> arrayData = new ArrayList<>();
        for (; ; ) {
            if (lexer.isCharacter('}')) {
                lexer.character();
                break;
            }
            if (lexer.isCharacter(',')) {
                lexer.character();
            }
            arrayData.add(subNestedType.deserializeText(lexer));
        }
        return new ClickHouseArray(subNestedType, arrayData.toArray());
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Long offset = offsetIDataType.deserializeBinary(deserializer);
        Object[] data = subNestedType.deserializeBinaryBulk(offset.intValue(), deserializer);
        return new ClickHouseArray(subNestedType, data);
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        ClickHouseArray[] arrays = new ClickHouseArray[rows];
        if (rows == 0) {
            return arrays;
        }

        int[] offsets = Arrays.stream(offsetIDataType.deserializeBinaryBulk(rows, deserializer)).mapToInt(value -> ((Long) value).intValue()).toArray();
        ClickHouseArray res = new ClickHouseArray(subNestedType,
                subNestedType.deserializeBinaryBulk(offsets[rows - 1], deserializer));

        for (int row = 0, lastOffset = 0; row < rows; row++) {
            int offset = offsets[row];
            arrays[row] = res.slice(lastOffset, offset - lastOffset);
            lastOffset = offset;
        }
        return arrays;
    }
}

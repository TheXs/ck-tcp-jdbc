package com.github.housepower.data;

import com.github.housepower.data.type.complex.DataTypeMap;
import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @ClassName: ColumnMap
 * @Description: TODO
 * @Author: Amitola
 * @Date: 2021/5/20
 **/
public class ColumnMap extends AbstractColumn {
    private final List<Long> offsets;
    // data represents nested column in ColumnArray
    private final IColumn data;

    public ColumnMap(String name, DataTypeMap type, Object[] values) {
        super(name, type, values);
        offsets = new ArrayList<>();
        data = ColumnFactory.createColumn(null, type.getElementType(), null);
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        if (object == null) {
            offsets.add(1l);
            data.write(new ClickHouseArray(data.type(), new Object[]{new HashMap<>()}));
        } else {
            /*Object[] arr = ((ClickHouseArray) object).getArray();
            offsets.add(offsets.isEmpty() ? arr.length : offsets.get((offsets.size() - 1)) + arr.length);*/
            //for (Object field : arr) {
                data.write(object);
           // }
        }
    }

    @Override
    public void flushToSerializer(BinarySerializer serializer, boolean now) throws IOException, SQLException {
        if (isExported()) {
            serializer.writeUTF8StringBinary(name);
            serializer.writeUTF8StringBinary(type.name());
        }

        flushOffsets(serializer);
        data.flushToSerializer(serializer, false);

        if (now) {
            buffer.writeTo(serializer);
        }
    }

    public void flushOffsets(BinarySerializer serializer) throws IOException {
        for (long offsetList : offsets) {
            serializer.writeLong(offsetList);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
        data.setColumnWriterBuffer(buffer);
    }

    @Override
    public void clear() {
        offsets.clear();
        data.clear();
    }
}

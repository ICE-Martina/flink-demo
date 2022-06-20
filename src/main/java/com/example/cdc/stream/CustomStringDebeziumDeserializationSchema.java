package com.example.cdc.stream;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author liuwe 2022/6/20 11:07
 */
public class CustomStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    /***
     * 数据格式:
     * {
     *     db: "",
     *     tableName: "",
     *     op: "",
     *     ts_ms: "",
     *     before: {"": "", ...},
     *     after: {"": "", ...}
     * }
     */
    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        JSONObject jsonObject = new JSONObject();
        String[] dbInfo = record.topic().split("\\.");
        String dbName = dbInfo[1];
        String tableName = dbInfo[2];
        jsonObject.put("db", dbName);
        jsonObject.put("table_name", tableName);

        Struct value = (Struct) record.value();
        // before数据
        jsonObject.put("op", value.getString("op"));
        jsonObject.put("ts_ms", value.getInt64("ts_ms"));
        Struct before = value.getStruct("before");
        jsonObject.put("before", toJsonBean(before));

        // after数据
        Struct after = value.getStruct("after");
        jsonObject.put("after", toJsonBean(after));

        out.collect(jsonObject.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    private static JSONObject toJsonBean(Struct struct) {
        JSONObject jsonObj = new JSONObject();
        if (struct != null) {
            Schema schema = struct.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                jsonObj.put(field.name(), struct.get(field));
            }
        }
        return jsonObj;
    }
}

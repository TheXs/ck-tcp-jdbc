package examples;


import com.github.housepower.data.IDataType;
import com.github.housepower.data.type.DataTypeUInt8;
import com.github.housepower.data.type.complex.DataTypeString;
import com.github.housepower.data.type.complex.DataTypeTuple;
import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.jdbc.ClickHouseStruct;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataImport {
    static String [] catTechniques = {"/BruteForce", "/BruteForce/Login", "/Brute Force/URLGuessing", "/DoS", "/Worm",
            "/Virus", "/CSRF", "/Cookie"};
    static String [] catBehaviors = {"/Found/Defective", "/Found/Misconfigured", "/Found/Exhausted", "/Found/NotWorking",
            "/Found/AccessoriesMissing", "/Found/Broken", "/Found/Smoking", "/Found/Leakage"};
    static String [] catOutComes = {"OK", "DANGER"};
    static String [] dataBaseType = {"mysql", "oracle", "sqlServer", "db2", "mongodb", "redis", "levelDB", "clickhouse","Hbase", "ElasticSearch"};
    static Map<Long, Map<String, Object>> dataSetMap = new HashMap<>();

    public static ClickHouseArray testTupleInsert() {
        Object [] tuple = new Object[2];
        tuple [0] = "1";
        tuple [1] = "Hello";
        IDataType [] nested = new IDataType[2];
        nested[0] = new DataTypeString(Charset.forName("UTF-8"));
        nested[1] = new DataTypeString(Charset.forName("UTF-8"));
        Object [] tuple1 = new Object[2];
        tuple1 [0] = "2";
        tuple1 [1] = "ack";
        IDataType [] nested1 = new IDataType[2];
        nested1[0] = new DataTypeUInt8();
        nested1[1] = new DataTypeString(Charset.forName("UTF-8"));
        ClickHouseStruct clickHouseStruct = new ClickHouseStruct("Tuple", tuple);
        ClickHouseStruct clickHouseStruct1 = new ClickHouseStruct("Tuple", tuple1);
        Object [] tupleArray = new Object[]{clickHouseStruct, clickHouseStruct1};
        ClickHouseArray clickHouseArray = new ClickHouseArray(new DataTypeTuple("Tuple", nested), tupleArray);
        return clickHouseArray;
    }
    public static Map<Long, Map<String, Object>> createDataByRows(long target) throws IOException, ParseException {
        Map<Long, Map<String, Object>> dataSetEach = new HashMap<>();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (long rows = 0; rows < target; rows++) {
            Map<String, Object> row = new HashMap<>();
            if(BatchQuery.insertNull) {
                Date startTime = new Date();
                int newMinute = startTime.getMinutes() + new Random().nextInt(60);
                startTime.setDate(startTime.getDay() + new Random().nextInt(30));
                startTime.setMinutes((startTime.getMinutes() + (newMinute >= 60 ? newMinute -= 60 : newMinute)));
                startTime.setMonth(startTime.getMonth());
                Date zonedDateTime = simpleDateFormat.parse(simpleDateFormat.format(startTime));
                row.put("startTime", zonedDateTime);
                row.put("deviceId", CommonUtils.randomLongGenerator(16));
            } else {
                row.put("logType", "audit");
                ZonedDateTime startTime = ZonedDateTime.now();
                startTime.plusDays(new Random().nextInt(30));
                startTime.plusMinutes(new Random().nextInt(60));
                // Date startTime = new Date();
                /*int newMinute = startTime.getMinutes() + new Random().nextInt(60);
                startTime.setDate(startTime.getDay() + new Random().nextInt(30));
                startTime.setMinutes((startTime.getMinutes() + (newMinute >= 60 ? newMinute -= 60 : newMinute)));
                startTime.setMonth(startTime.getMonth());*/
                row.put("startTime", startTime);
                row.put("sqlLen", 232);
                row.put("clientUserName", "XS" + new Random().nextInt(1000));
                row.put("srcHostName", "XSHost" + new Random().nextInt(1000));
                row.put("dvcAction", "SELECT");
                row.put("destMacAddress", "AE-05-E3-9B-5C-" + new Random().nextInt(100));
                row.put("dataBaseName", "XS" + new Random().nextInt(10));
                row.put("srcMacAddress", "AE-05-E3-9B-5C-" + new Random().nextInt(100));
                row.put("destAddress", "192.168.0." + new Random().nextInt(100));
                row.put("logSessionId", CommonUtils.randomLongGenerator(16));
                row.put("alarmFlag", new Random().nextInt(2));
                row.put("responseCode", new Random().nextInt(100));
                row.put("destPort", new Random().nextInt(10000));
                row.put("srcUserName", "XS" + new Random().nextInt(100));
                row.put("relateIp", "192.168.2." + new Random().nextInt(100));
                row.put("payload", "select b.client_tcp_port, b.client_net_address, b.connect_time, a.host_name, a.program_name, a.login_name from master.sys.dm_exec_sessions a, master.sys.dm_exec_connections b where a.session_id=b.session_id and client_tcp_port!=0");
                row.put("severity", new Random().nextInt(10));
                row.put("srcAddress", "192.168.4." + new Random().nextInt(100));
                row.put("clientPrg", "audit");
                row.put("dataSet", "client_tcp_port ~ client_net_address ~ connect_time ~ host_name ~ program_name ~ login_name |E$$L| 59031 ~ 192.168.50.33 ~ 2015-5-15 20:34:41.570 ~ dbone-rivorhua ~ NULL ~ sa |E$$L| 51165 ~ 192.168.50.32 ~ 2015-5-15 21:15:52.256 ~ dbone32 ~ NULL ~ sa |E$$L| 3740 ~ 192.168.50.81 ~ 2015-5-15 21:18:32.680 ~ DBAPP-81 ~ Microsoft SQL Server Management Studio ~ 079_YL-MSSQLSER\\\\\\\\Administrator |E$$L| 3757 ~ 192.168.50.81 ~ 2015-5-15 21:19:27.680 ~ DBAPP-81 ~ Microsoft SQL Server Management Studio - 查询 ~ 079_YL-MSSQLSER\\\\\\\\Administrator |E$$L| ");
                row.put("relateUser", "XS" + new Random().nextInt(100));
                row.put("tenant", "local");
                row.put("sqlId", CommonUtils.randomLongGenerator(16));
                row.put("costTime", new Random().nextInt(1000));
                row.put("databaseType", dataBaseType[new Random().nextInt(dataBaseType.length)]);
                row.put("ruleName", "SELECT");
                row.put("errorMessage", "this is an error message sent from device，");
                row.put("accessId", CommonUtils.randomLongGenerator(16));
                row.put("relateUserInfo", "XS" + new Random().nextInt(100) + "is a good boy");
                row.put("effectRow", new Random().nextInt(100));
                row.put("srcPort", new Random().nextInt(10000));
                row.put("dataSetSize", new Random().nextInt(100000));
                row.put("relateUrl", "www.baidu.com");
                row.put("databaseObject", "some memory addresses");
                row.put("requestUrl", "www.facebook.com");
                row.put("name", "XS" + new Random().nextInt(100));
                row.put("message", "来源地址/端口：192.168.50.33/59031,目的地址/端口：192.168.50.79/1433,操作数据库对象：，操作类型：SELECT，执行结果：1,是否告警：0");
                row.put("deviceAddress", "192.168.30." + new Random().nextInt(1000));
                row.put("productVendorName", "安恒");
                row.put("deviceSendProductName", "DBAudit");
                row.put("deviceSendProductVersion", "v4.0." + new Random().nextInt(100));
                row.put("collectorReceiptTime", startTime);
                row.put("deviceReceiptTime", startTime);
                ZonedDateTime endTime = startTime;
                endTime.plusMinutes(10);
                row.put("endTime", endTime);
                row.put("sendHostAddress", "192.168.30." + new Random().nextInt(100));
                row.put("deviceCat", "/Audit/Database");
                row.put("catObject", "/Host/Application/Database" + (new Random().nextInt(10)));
                row.put("catTechnique", catTechniques[new Random().nextInt(catTechniques.length)]);
                row.put("catBehavior", catBehaviors[new Random().nextInt(catBehaviors.length)]);
                row.put("catSignificance", "/Informational");
                row.put("catOutcome", catOutComes[new Random().nextInt(catOutComes.length)]);
                row.put("dataType", "dbAudit");
                row.put("dataSubType", "audit");
                row.put("direction", 00);
                row.put("eventCount", new Random().nextInt(1000));
                row.put("eventId", CommonUtils.randomLongGenerator(16));

                row.put("deviceId", CommonUtils.randomLongGenerator(16));
                row.put("map", "I am a String");
                row.put("integer", "1");
            }
           /* int randomAddColumns = new Random().nextInt(150) + 50;
            for (int i = 0; i < randomAddColumns; i++) {
                row.put("randomColumn" + new Random().nextInt(200), new Random().nextInt(100000));
            }*/
            dataSetEach.put(rows, row);
            // dataSetMap.put(rows, row);
        }
        return dataSetEach;
    }

}

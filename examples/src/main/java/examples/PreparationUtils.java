package examples;

import com.github.housepower.jdbc.ClickHouseResultSetMetaData;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @ClassName: PreparationUitl
 * @Description: TODO
 * @Author: Amitola
 * @Date: 2021/4/30
 **/
class PreparationUtils {
    public static void preparationCheck(Connection connection, String tableName) throws SQLException {
        boolean execute = connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "(\n" +
                "logType Nullable(String),\n" +
                "startTime DateTime,\n" +
                "sqlLen Nullable(String),\n" +
                "clientUserName Nullable(String),\n" +
                "srcHostName Nullable(String),\n" +
                "dvcAction Nullable(String),\n" +
                "destMacAddress Nullable(String),\n" +
                "dataBaseName Nullable(String),\n" +
                "srcMacAddress Nullable(String),\n" +
                "destAddress Nullable(String),\n" +
                "logSessionId Nullable(Int64),\n" +
                "alarmFlag Nullable(UInt8),\n" +
                "responseCode Nullable(UInt8),\n" +
                "destPort Nullable(UInt8),\n" +
                "srcUserName Nullable(String),\n" +
                "relateIp Nullable(String),\n" +
                "payload Nullable(String),\n" +
                "severity Nullable(UInt8),\n" +
                "srcAddress Nullable(String),\n" +
                "clientPrg Nullable(String),\n" +
                "dataSet Nullable(String),\n" +
                "relateUser Nullable(String),\n" +
                "tenant Nullable(String),\n" +
                "sqlId Nullable(Int64),\n" +
                "costTime Nullable(Int64),\n" +
                "databaseType Nullable(String),\n" +
                "ruleName Nullable(String),\n" +
                "errorMessage Nullable(String),\n" +
                "accessId Nullable(Int64),\n" +
                "relateUserInfo Nullable(String),\n" +
                "effectRow Nullable(Int64),\n" +
                "srcPort Nullable(UInt8),\n" +
                "dataSetSize Nullable(Int64),\n" +
                "relateUrl Nullable(String),\n" +
                "databaseObject Nullable(String),\n" +
                "requestUrl Nullable(String),\n" +
                "name Nullable(String),\n" +
                "message Nullable(String),\n" +
                "deviceAddress Nullable(String),\n" +
                "productVendorName Nullable(String),\n" +
                "deviceSendProductName Nullable(String),\n" +
                "deviceSendProductVersion Nullable(String),\n" +
                "collectorReceiptTime Nullable(String),\n" +
                "deviceReceiptTime Nullable (DateTime),\n" +
                "endTime Nullable (DateTime),\n" +
                "sendHostAddress Nullable(String),\n" +
                "deviceCat Nullable(String),\n" +
                "catObject Nullable(String),\n" +
                "catTechnique Nullable(String),\n" +
                "catBehavior Nullable(String),\n" +
                "catSignificance Nullable(String),\n" +
                "catOutcome Nullable(String),\n" +
                "dataType Nullable(String),\n" +
                "dataSubType Nullable(String),\n" +
                "direction Nullable(UInt8),\n" +
                "eventCount Nullable(Int64),\n" +
                "eventId Nullable(Int64),\n" +
                "deviceName Nullable(String),\n" +
                "deviceId UInt64 ,\n" +
                "other Map(String, String) \n" +
                ")\n" +
                "ENGINE = MergeTree()\n" +
                "PARTITION BY toYYYYMMDD(startTime)\n" +
                "ORDER BY (deviceId, startTime)\n" +
                "SAMPLE BY deviceId\n" +
                "SETTINGS index_granularity = 8192").execute();

            columnsConfigure(connection, tableName);


    }

    public static void columnsConfigure(Connection connection, String tableName) throws SQLException {
        ClickHouseResultSetMetaData metaData = (ClickHouseResultSetMetaData) connection.prepareStatement("select * from " + tableName + " limit 0, 1").executeQuery().getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            BatchQuery.columns.add(metaData.getColumnName(i));
            BatchQuery.columnsInRecord.add(metaData.getColumnName(i));
        }
        System.out.println("columns updated");
    }
}

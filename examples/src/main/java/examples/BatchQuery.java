package examples;

import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.jdbc.ClickHouseConnection;
import com.github.housepower.jdbc.statement.ClickHousePreparedInsertStatement;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static examples.DataImport.testTupleInsert;

/**
 */
public class BatchQuery {

    static int batch_count = 20000;
    static Set<String> columnsInRecord = new HashSet<>();
    static List<String> columns = new ArrayList<>();
    static String insertSql;
    static int targetRows = 200000;
    static boolean insertNull = false;
    static int columnsToAdd = 0;
    static String tableName = "map";
    static CountDownLatch countDownLatch;

    public static void main(String[] args) throws Exception {
        long totalAvgTime = 0;
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.30.12:9000");

        /*if(columnsToAdd > 0) {
            for (int i = 0; i < columnsToAdd; i++) {
                String columnName = "AddedColumn" + (columns.size() - 59);
                addColumnProcess(connection, tableName, ClickHouseDataType.String);
            }
        }*/

        PreparationUtils.preparationCheck(connection, "map");

        createSql();

        // PreparedStatement pstmt = connection.prepareStatement(insertSql);

        int batchCycle = targetRows / batch_count;

        countDownLatch = new CountDownLatch(1);

        long start = System.currentTimeMillis();
        //for (int i = 0; i < batchCycle; i++) {
             //new InsertTestTask().start();
           /* DataImport.createDataByRows(batch_count);
            Iterator<Map<String, Object>> iterator = DataImport.dataSetMap.values().iterator();
            while(iterator.hasNext()) {
                Map<String, Object> next = iterator.next();
                dataInitialization(pstmt);
                for(String key : next.keySet()) {
                    int index = columns.indexOf(key) + 1;
                    pstmt.setObject(index, next.get(key));
                }
                // 如果需要更新sql
                pstmt.addBatch();
            }
            pstmt.executeBatch();*/
           /* long start = System.currentTimeMillis();
            pstmt.executeBatch();
            long end = System.currentTimeMillis();
            totalAvgTime += (end - start);
            DataImport.dataSetMap.clear();*/
       // }
        ClickHouseArray clickHouseArray = testTupleInsert();
        ClickHousePreparedInsertStatement pstmt = (ClickHousePreparedInsertStatement) connection.prepareStatement(insertSql);
        pstmt.setObject(1, clickHouseArray);
        pstmt.execute();
        //countDownLatch.await();
        long end = System.currentTimeMillis();
        totalAvgTime += (end - start);
        System.out.println("insert cost(ms): " + totalAvgTime/batchCycle);
        connection.close();
    }

    private static void dataInitialization(PreparedStatement preparedStatement) throws SQLException {
        for(int i = 0; i < columns.size(); i++) {
            preparedStatement.setObject(i + 1, null);
        }
    }

    private static void createSql() {
        StringBuilder headPart = new StringBuilder("insert into " + tableName + " (");
        StringBuilder valuesPart = new StringBuilder("VALUES(");
        for (int i = 0; i < columns.size(); i++) {
            if(i != columns.size() - 1) {
                headPart.append(columns.get(i)).append(",");
                valuesPart.append("?,");
            } else {
                headPart.append(columns.get(i)).append(") ");
                valuesPart.append("?);");
            }
        }
        headPart.append(valuesPart);
        insertSql = headPart.toString();
    }

    private static void addColumnProcess(Connection connection, String newColumnName, ClickHouseDataType clickHouseDataType) throws SQLException {
        columnsInRecord.add(newColumnName);
        columns.add(newColumnName);
        connection.prepareStatement("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + newColumnName
                + " Nullable(" + clickHouseDataType + ") AFTER " + BatchQuery.columns.get(columns.size() - 2) + ";").execute();
        createSql();
    }

    static class InsertTestTask extends Thread {
        ThreadLocal<Map<Long, Map<String, Object>>>local = new ThreadLocal<>();
        Connection connection;

        InsertTestTask(){}

        @Override
        public void run() {
            try {
                this.local.set(DataImport.createDataByRows(batch_count));
                Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
                ClickHouseConnection connection = (ClickHouseConnection) DriverManager.getConnection("jdbc:clickhouse://192.168.30.12:9000");
                ClickHousePreparedInsertStatement pstmt = (ClickHousePreparedInsertStatement) connection.prepareStatement(insertSql);
                Iterator<Map<String, Object>> iterator = this.local.get().values().iterator();
                while(iterator.hasNext()) {
                    boolean otherPresented = false;
                    Map<String, Object> otherFiled = new HashMap<>();
                    Map<String, Object> next = iterator.next();
                    //dataInitialization(pstmt);
                    for(String key : next.keySet()) {
                        int index = columns.indexOf(key);
                        if(index == -1) {
                            otherPresented = true;
                            otherFiled.put(key, next.get(key));
                            continue;
                        }
                        pstmt.setObject(index + 1, next.get(key));
                    }
                    if(otherPresented) {
                        pstmt.setObject(columns.indexOf("other") + 1, otherFiled);
                    }
                    // 如果需要更新sql
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                pstmt.close();
                countDownLatch.countDown();

            } catch (IOException | SQLException | ClassNotFoundException | ParseException e) {
                e.printStackTrace();
            }
        }
    }

}

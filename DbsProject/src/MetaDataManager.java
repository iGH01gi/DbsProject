import java.sql.*;
import java.util.List;
import java.util.Map;

public class MetaDataManager {
    private static final String URL = "jdbc:mysql://localhost:3306/dbsproject";
    private static final String USER = "root";
    private static final String PASSWORD = "igh17172080";
    private Connection _connection;
    private static final String _relationMetaTable = "relation_metadata"; //테이블 메타데이터를 저장하는 테이블 이름
    private static final String _attributeMetaTable = "attribute_metadata"; //컬럼 메타데이터를 저장하는 테이블 이름
    private static final String FILE_PATH = "/files/"; //파일들이 저장될 경로

    public MetaDataManager() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            _connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (Exception e) {
            System.out.println("MySQL connection failed!");
            e.printStackTrace();
        }
    }

    /**
     * 테이블 메타데이터를 저장하는 테이블에 새로운 레코드를 추가
     *
     * @param tableName
     * @param pkColumns
     */
    public void InsertRelationMetadata(String tableName, Map<String, Integer> columns, List<String> pkColumns) {
        String sql = "INSERT INTO " + _relationMetaTable + " (relation_name, attribute_num, length, location) VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = _connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            pstmt.setInt(2, columns.size());
            pstmt.setInt(3, columns.values().stream().mapToInt(Integer::intValue).sum());
            pstmt.setString(4, FILE_PATH + tableName);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Insert into relation_metadata failed!");
            e.printStackTrace();
        }
    }

    /**
     * 컬럼 메타데이터를 저장하는 테이블에 새로운 레코드를 추가
     *
     * @param tableName
     * @param columns
     * @param pkColumns
     */
    public void InsertAttributeMetadata(String tableName, Map<String, Integer> columns, List<String> pkColumns) {
        String sql = "INSERT INTO " + _attributeMetaTable + " (relation_name, attribute_name, is_PK, position, length) VALUES (?, ?, ?, ?, ?)";
        int position = 1;

        for (Map.Entry<String, Integer> entry : columns.entrySet()) {
            try (PreparedStatement pstmt = _connection.prepareStatement(sql)) {
                pstmt.setString(1, tableName);
                pstmt.setString(2, entry.getKey());
                pstmt.setBoolean(3, pkColumns.contains(entry.getKey()));
                pstmt.setInt(4, position++);
                pstmt.setInt(5, entry.getValue());

                pstmt.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Insert into attribute_metadata failed!");
                e.printStackTrace();
            }
        }
    }
    
    public ResultSet SearchTable(String tableName, List<String> condition) {
        
        //where 절이 필요없을 경우, 테이블 전체를 결과셋으로 리턴
        if(condition == null){
            String sql = "SELECT * FROM " + tableName;
            ResultSet rs = null;
            
            try (PreparedStatement pstmt = _connection.prepareStatement(sql)) {
                rs = pstmt.executeQuery();
            } catch (SQLException e) {
                System.out.println("Search table failed!");
                e.printStackTrace();
            }
            
            return rs;
        }
        else {
            //where 절이 필요할 경우, 조건에 맞는 결과셋을 리턴
            String sql = "SELECT * FROM " + tableName + " WHERE ";
            for(int i = 0; i < condition.size(); i++){
                sql += condition.get(i);
                if(i != condition.size() - 1){
                    sql += " AND ";
                }
            }
            ResultSet rs = null;
            
            try (PreparedStatement pstmt = _connection.prepareStatement(sql)) {
                rs = pstmt.executeQuery();
            } catch (SQLException e) {
                System.out.println("Search table with condition failed!");
                e.printStackTrace();
            }
        }
    }


}

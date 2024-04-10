import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MetaDataManager {
    private static final String URL = "jdbc:mysql://localhost:3306/dbsproject";
    private static final String USER = "root";
    private static final String PASSWORD = "igh17172080";
    private static final String _relationMetaTable = "relation_metadata"; //테이블 메타데이터를 저장하는 테이블 이름
    private static final String _attributeMetaTable = "attribute_metadata"; //컬럼 메타데이터를 저장하는 테이블 이름
    private static final String FILE_PATH = "files/"; //파일들이 저장될 경로
    private Connection _connection;

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

    /**
     * 메타데이터를 검색
     *
     * @param tableName 메타데이터 테이블의 이름
     * @param condition Where 절에 사용할 조건식들 (ex. "relation_name = '테이블이름'"). 필요없을 경우 null
     * @return ResultList을 반환 (없을시 빈 리스트 반환)
     */
    public List<LinkedHashMap<String, String>> SearchTable(String tableName, List<String> condition) {

        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        ResultSet rs = null;

        try {
            //where 절이 필요없을 경우, 테이블 전체를 결과셋으로 리턴
            if (condition == null) {
                String sql = "SELECT * FROM " + tableName;


                try (PreparedStatement pstmt = _connection.prepareStatement(sql)) {
                    rs = pstmt.executeQuery();
                    list = ResultSetToResultList(rs);
                } catch (SQLException e) {
                    /*System.out.println("Search table failed!");
                    e.printStackTrace();*/
                }
                
                return list;
            } else {
                //where 절이 필요할 경우, 조건에 맞는 결과셋을 리턴
                String sql = "SELECT * FROM " + tableName + " WHERE ";
                for (int i = 0; i < condition.size(); i++) {
                    sql += condition.get(i);
                    if (i != condition.size() - 1) {
                        sql += " AND ";
                    }
                }

                try (PreparedStatement pstmt = _connection.prepareStatement(sql)) {
                    rs = pstmt.executeQuery();
                    list = ResultSetToResultList(rs);
                } catch (SQLException e) {
                    /*System.out.println("Search table with condition failed!");
                    e.printStackTrace();*/
                }
                
                return list;
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    /*System.out.println("ResultSet close failed!");
                    e.printStackTrace();*/
                }
            }
        }
    }

    /**
     * ResultSet을 List로 변환
     *
     * @param rs ResultSet
     * @return List<LinkedHashMap < String, String> 순서가 보장되는 LinkedHashMap의 List. ResultSet이 비어있을 경우 빈 리스트 반환
     */
    public List<LinkedHashMap<String, String>> ResultSetToResultList(ResultSet rs) {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();

        try {
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();

            while (rs.next()) {
                LinkedHashMap<String, String> row = new LinkedHashMap<>();
                for (int i = 1; i <= columns; ++i) {
                    row.put(md.getColumnName(i), rs.getString(i));
                }
                list.add(row);
            }
        } catch (SQLException e) {
            System.out.println("ResultSetToList failed!");
            e.printStackTrace();
        }

        return list;
    }


}

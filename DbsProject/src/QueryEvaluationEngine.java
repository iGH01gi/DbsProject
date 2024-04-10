import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 실질적인 쿼리 처리를 담당하는 클래스
 */
public class QueryEvaluationEngine {
    
    private BufferManager _bufferManager; // 고정길이 레코드 파일 Block I/O를 수행하는 인스턴스
    public MetaDataManager _metaDataManager; // MySQL에 저장된 메타데이터를 다루는 인스턴스
    private static final String FILE_PATH = "files/"; //파일들이 저장될 경로
    private static final String _relationMetaTable = "relation_metadata"; //테이블 메타데이터를 저장하는 테이블 이름
    private static final String _attributeMetaTable = "attribute_metadata"; //컬럼 메타데이터를 저장하는 테이블 이름
    
    public QueryEvaluationEngine() {
        _bufferManager = new BufferManager();
        _metaDataManager = new MetaDataManager();
    }
    
    public void CreateTable(String tableName, Map<String, Integer> columns, List<String> pkColumns) {
        //테이블 메타데이터를 MySQL에 저장
        _metaDataManager.InsertRelationMetadata(tableName, columns, pkColumns);
        
        //컬럼 메타데이터를 MySQL에 저장
        _metaDataManager.InsertAttributeMetadata(tableName, columns, pkColumns);
        
        //실제 파일 생성
        _bufferManager.CreateFile(tableName, columns);
    }

    public void InsertTuple() {
    }

    public void DeleteTuple() {
    }

    public void SearchTable() {
    }

    public void SearchTupleWithPk() {
    }
    
    /**
     * 테이블이 존재하는지 확인<br>
     * 메타데이터 + 실제 파일이 존재해야 함
     * @param tableName 테이블명이자 파일명
     * @return 테이블이 존재하면 true, 존재하지 않으면 false
     */
    public boolean IsTableExist(String tableName) {
        boolean isExist = true;
        
        //메타데이터가 존재하는지 확인
        List<String> condition = new ArrayList<>();
        condition.add("relation_name = " +  "'" + tableName +"'");
        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_relationMetaTable,condition);
        try {
            if(rl.isEmpty()) {
                System.out.println("##해당 테이블에대한 메타데이터가 없음");
                isExist = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        //실제 파일이 존재하는지 확인
        File file = new File(FILE_PATH + tableName);
        if(file.exists()) {
            
        } else {
            System.out.println("##해당 테이블 파일이 없음. ("+ FILE_PATH + tableName + " 파일을 찾을 수 없음)");
            isExist = false;
        }
        
        return isExist;

    }

    /**
     * 실제파일+메타데이터가 존재하는 테이블명을 출력
     * @return 테이블이 하나도 없으면 false, 있으면 true
     */
    public boolean PrintAllTableNames() {

        List<String> tableNames = new ArrayList<>();

        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_relationMetaTable, null);

        if (rl.isEmpty()) {
            System.out.println("[현재 존재하는 테이블이 없음]");
            return false;
        } else {
            for (int i = 0; i < rl.size(); i++) {
                //메타데이터 검색 결과리스트에서, 현재 row의 relation_name을 가져옴
                String tableName = rl.get(i).get("relation_name");

                //실제 파일이 존재해야지만 출력가능
                File file = new File(FILE_PATH + tableName);
                if (file.exists()) {
                    tableNames.add(tableName);
                }
            }

            if (tableNames.size() == 0) {
                System.out.println("[현재 존재하는 테이블이 없음]");
                return false;
            } else {
                System.out.println("[현재 존재하는 테이블 목록]");
                for (String tableName : tableNames) {
                    System.out.println(" -> " + tableName);
                }
                return true;
            }
        }
    }
        
}

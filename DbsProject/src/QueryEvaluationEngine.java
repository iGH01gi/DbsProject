import java.util.List;
import java.util.Map;

/**
 * 실질적인 쿼리 처리를 담당하는 클래스
 */
public class QueryEvaluationEngine {
    
    private BufferManager _bufferManager; // 고정길이 레코드 파일 Block I/O를 수행하는 인스턴스
    public MetaDataManager _metaDataManager; // MySQL에 저장된 메타데이터를 다루는 인스턴스
    private static final String FILE_PATH = "files/"; //파일들이 저장될 경로
    
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
    
    public boolean IsTableExist(String tableName) {
        _metaDataManager.SearchTable(tableName,null);

    }
}

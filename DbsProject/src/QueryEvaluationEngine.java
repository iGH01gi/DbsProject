import java.io.File;
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

    public void InsertTuple(String tableName, List<String> newColumnValues) {
        //헤더 레코드에 담겨있는 값을 읽어옴
        int headerRecordValue = -1;
        byte[] firstBlock = _bufferManager.ReadBlockFromFile(tableName, 0);
        if(firstBlock==null) {
            System.out.println("##해당 파일의 헤더 레코드를 읽어오는데 실패함");
            return;
        }
        int recordLength = GetRecordLength(tableName);
        if(recordLength== -1) {
            System.out.println("##해당 테이블의 레코드 길이를 구하는데 실패함");
            return;
        }
        headerRecordValue = _bufferManager.GetFreeRecordValue(firstBlock,0,recordLength);
        
        
        //헤더 레코드에 담겨있는 값으로, 실제 레코드를 저장할 위치를 계산
        int blockingFactor = _bufferManager.BLOCK_SIZE / recordLength;
        int blockIndex = headerRecordValue / blockingFactor; //파일에서 저장될 block의 index(0부터 시작)
        int recordIndex = headerRecordValue % blockingFactor; //block내에서 저장될 record의 index(0부터 시작)
        
        
        //헤더레코드가 포인팅하던 free record가 file 범위를 벗어난다면
        //헤더가 포인팅 하는 곳이 free-record-list의 유일한 노드임 (즉, 중간에 빈 레코드가 없음)
        //따라서 새로운 block을 '생성'하고 -> 생성한 block에 레코드를 삽입 -> block을 파일에 write -> 헤더 레코드에 새로운 값(기존 값+1)을 저장
        if(_bufferManager.BLOCK_SIZE * blockIndex >= GetFileSize(tableName)) {
            byte[] newBlock = _bufferManager.CreateBlock();
            if(!_bufferManager.WriteRecordToBlock(newBlock, recordLength, newColumnValues, recordIndex)){
                System.out.println("##레코드를 쓰는데 실패함");
                return;
            }
            _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), newBlock, blockIndex);
            
            //헤더 레코드에 새로운 값(기존 값+1)을 저장
            byte[] _firstBlock = _bufferManager.ReadBlockFromFile(tableName, 0);
            _bufferManager.SetFreeRecordValue(_firstBlock, 0, headerRecordValue + 1, recordLength);
            _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), _firstBlock, 0);
        }

        //헤더레코드가 포인팅하던 free record가 지닌 값이 '0' 이라면
        //헤더가 포인팅 하는 곳이 free-record-list의 유일한 노드임 (즉, 중간에 빈 레코드가 없음)
        //따라서 해당 block을 '읽고' -> block에 레코드를 삽입 -> block을 파일에 write -> 헤더 레코드에 새로운 값(기존 값+1)을 저장
        else if (_bufferManager.GetFreeRecordValue(_bufferManager.ReadBlockFromFile(tableName, blockIndex), recordIndex, recordLength) == 0) {
            byte[] block = _bufferManager.ReadBlockFromFile(tableName, blockIndex);
            if(!_bufferManager.WriteRecordToBlock(block, recordLength, newColumnValues, recordIndex)){
                System.out.println("##레코드를 쓰는데 실패함");
                return;
            }
            _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), block, blockIndex);

            //헤더 레코드에 새로운 값(기존 값+1)을 저장
            byte[] _firstBlock = _bufferManager.ReadBlockFromFile(tableName, 0);
            _bufferManager.SetFreeRecordValue(_firstBlock, 0, headerRecordValue + 1, recordLength);
            _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), _firstBlock, 0);
        }


        //헤더레코드가 포인팅하던 free record가 file범위를 벗어나지 않고 지닌 값이 '0'이 아니라면
        //테이블 중간중간 빈 공간이 있는 상태라는 것
        //따라서 해당 block을 '읽고' -> block에 레코드를 삽입 -> block을 파일에 write -> 헤더 레코드에 새로운 값(해당 free record에 들어있던 값)을 저장
        else {
            byte[] block = _bufferManager.ReadBlockFromFile(tableName, blockIndex);
            int existingFreeRecordValue = _bufferManager.GetFreeRecordValue(block, recordIndex, recordLength);
            if(!_bufferManager.WriteRecordToBlock(block, recordLength, newColumnValues, recordIndex)){
                System.out.println("##레코드를 쓰는데 실패함");
                return;
            }
            _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), block, blockIndex);

            //헤더 레코드에 새로운 값(해당 free record에 들어있던 값)을 저장
            byte[] _firstBlock = _bufferManager.ReadBlockFromFile(tableName, 0);
            _bufferManager.SetFreeRecordValue(_firstBlock, 0, existingFreeRecordValue, recordLength);
            _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), _firstBlock, 0);
        }

        System.out.println("\n@@@레코드 삽입 성공@@@");
        
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
        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_relationMetaTable, condition,null);
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

        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_relationMetaTable, null, null);

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
    
    /**
     * 테이블의 컬럼명을 출력 (글자제한도 포함해서)
     * @param tableName 테이블명
     * @return 컬럼메타데이터가 있어서 정상출력이면 true, 없으면 false
     */
    public boolean PrintColumnNames(String tableName) {
        List<String> condition = new ArrayList<>();
        condition.add("relation_name = " +  "'" + tableName +"'");
        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_attributeMetaTable,condition, "position ASC");
        int rows = rl.size();
        
        if(rl.isEmpty()) {
            System.out.println("##해당 테이블에대한 컬럼 메타데이터가 없음");
            return false;
        } else {
            System.out.println("[ " + tableName + " 테이블의 컬럼 목록 ]");
            for (int i = 0; i < rows; i++) {
                //rl.get(i)에서 attribute_name을 가져와서 출력
                System.out.println(" -> 컬럼명: " + rl.get(i).get("attribute_name") + ", 글자수 제한: " + rl.get(i).get("length"));
            }
            return true;
        }
    }
    
    /**
     * 테이블의 컬럼정보(key:컬럼명, value:글자수 제한)를 가져오는 메소드
     * @param tableName 테이블명
     * @return 순서가 보장되는 컬럼정보 맵. 메타데이터에 없으면 빈 맵 반환
     */
    public LinkedHashMap<String,String> GetColumnInfo(String tableName) {
        List<String> condition = new ArrayList<>();
        condition.add("relation_name = " +  "'" + tableName +"'");
        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_attributeMetaTable,condition, "position ASC");
        int rows = rl.size();

        LinkedHashMap<String,String> columnInfo = new LinkedHashMap<>();

        if(rl.isEmpty()) {
            return columnInfo;
        } else {
            for (int i = 0; i < rows; i++) {
                columnInfo.put(rl.get(i).get("attribute_name"), rl.get(i).get("length"));
            }
            return columnInfo;
        }
    }
    
    /**
     * 테이블의 레코드 길이를 가져오는 메소드
     * @param tableName 테이블명
     * @return 레코드 길이. 메타데이터가 없어서 구할 수 없으면 -1 반환
     */
    public int GetRecordLength(String tableName) {
        LinkedHashMap<String,String> columnInfo = GetColumnInfo(tableName);
        
        if(columnInfo.isEmpty()) {
            return -1;
        }
        
        int recordLength = columnInfo.values().stream().mapToInt(Integer::parseInt).sum();
        return recordLength;
    }
    
    /**
     * 파일의 크기를 가져오는 메소드
     * @param tableName 테이블명
     * @return 파일의 크기(bytes)
     */
    public long GetFileSize(String tableName) {
        File file = new File(FILE_PATH + tableName);
        return (long)file.length();
    }
        
}

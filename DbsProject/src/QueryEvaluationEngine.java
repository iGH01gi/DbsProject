import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

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

    public void InsertTuple(String tableName, LinkedHashMap<String,String> newColumnValueInfo) {
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
            if(!_bufferManager.WriteRecordToBlock(newBlock, recordLength, newColumnValueInfo, recordIndex)){
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
            if(!_bufferManager.WriteRecordToBlock(block, recordLength, newColumnValueInfo, recordIndex)){
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
            if(!_bufferManager.WriteRecordToBlock(block, recordLength, newColumnValueInfo, recordIndex)){
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

    /**
     * 튜플 삭제
     * @param tableName 테이블명
     * @param pkInfoForDeletion PK 컬럼정보(key:pk값, value:글자수제한)
     */
    public void DeleteTuple(String tableName, LinkedHashMap<String, String> pkInfoForDeletion) {
        
        //Key: 검색된 레코드가 위치한 Block 인덱스. Value: 해당 Block에서 검색된 레코드의 인덱스
        //검색된 레코드가 없으면 null
        LinkedHashMap<Integer,Integer> recordInfo = SearchTupleWithPkNoPrint(tableName, pkInfoForDeletion);
        
        if(recordInfo==null || recordInfo.isEmpty()) {
            System.out.println("##삭제하고자 하는 레코드가 없음");
            return;
        }
        
        int blockIndex = recordInfo.keySet().toArray(new Integer[1])[0];
        int recordIndex = recordInfo.values().toArray(new Integer[1])[0];
        int blockingFactor = _bufferManager.BLOCK_SIZE / GetRecordLength(tableName);
        
        //기존 헤드 레코드값을 기록해 둠 
        byte[] firstBlock = _bufferManager.ReadBlockFromFile(tableName, 0);
        int oldHeaderRecordValue = _bufferManager.GetFreeRecordValue(firstBlock,0,GetRecordLength(tableName));
        
        //삭제할 레코드의 위치를 헤더 레코드에 기록하고 파일에 쓰기
        int deletedFreeNodeIndex = blockingFactor * blockIndex + recordIndex;
        _bufferManager.SetFreeRecordValue(firstBlock, 0,deletedFreeNodeIndex , GetRecordLength(tableName));
        _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), firstBlock, 0);
        
        //삭제할 레코드의 위치에 기존 헤더 레코드값을 기록하고 파일에 쓰기
        byte[] searchBlock = _bufferManager.ReadBlockFromFile(tableName, blockIndex);
        _bufferManager.SetFreeRecordValue(searchBlock,recordIndex, oldHeaderRecordValue, GetRecordLength(tableName));
        _bufferManager.WriteBlockToFile(new File(FILE_PATH + tableName), searchBlock, blockIndex);

        System.out.println("\n@@@레코드 삭제 성공@@@");
        
    }

    public void SearchTable(String tableName) {
        System.out.println("\n [ " + tableName + " 테이블의 전체 튜플 검색 ]");
        
        LinkedHashMap<String,String> columnInfo = GetColumnInfo(tableName);
        int columnNum = columnInfo.size();
        int recordLength = GetRecordLength(tableName);
        int blockingFactor = _bufferManager.BLOCK_SIZE / recordLength;
        
        if(columnInfo.isEmpty()) {
            System.out.println("##해당 테이블의 컬럼 메타데이터가 없음");
            return;
        }
        
        //컬럼명 순서대로 출력
        for (Map.Entry<String, String> entry : columnInfo.entrySet()) {
            System.out.print(entry.getKey() + "  |  ");
        }
        System.out.println();
        
        //파일에서 block을 읽어오고, block내의 모든 레코드를 출력
        // 파일의 block의 총 갯수
        int blockSum = (int)GetFileSize(tableName) / _bufferManager.BLOCK_SIZE;
        
        // 파일의 block의 총 갯수만큼 반복
        for(int i=0; i<blockSum; i++){
            byte[] block = _bufferManager.ReadBlockFromFile(tableName, i);
            for(int j=0; j<blockingFactor; j++){
                byte[] record = _bufferManager.ReadRecordFromBlock(block, recordLength, j);
                if(record != null && !_bufferManager.IsFreeRecord(record)){
                    PrintRecord(record, columnInfo);
                    System.out.println();
                }
            }
            System.out.print(i + "번째 블록의 레코드 출력 완료\n");
        }
        
    }

    /**
     * 특정 테이블의 튜플 한 개 검색
     * @param tableName 테이블명
     * @param pkInfoForSearch PK 컬럼정보(key:pk값, value:글자수제한)
     */
    public void SearchTupleWithPk(String tableName, LinkedHashMap<String, String> pkInfoForSearch) {
        LinkedHashMap<String,String> columnInfo = GetColumnInfo(tableName);
        int recordLength = GetRecordLength(tableName);
        int blockingFactor = _bufferManager.BLOCK_SIZE / recordLength;

        if(columnInfo.isEmpty()) {
            System.out.println("##해당 테이블의 컬럼 메타데이터가 없음");
            return;
        }

        //컬럼명 순서대로 출력
        for (Map.Entry<String, String> entry : columnInfo.entrySet()) {
            System.out.print(entry.getKey() + "  |  ");
        }
        System.out.println();
        
        // 파일의 block의 총 갯수
        int blockSum = (int)GetFileSize(tableName) / _bufferManager.BLOCK_SIZE;

        // 검색값(pk값)에 해당하는 record가 나올때까지 block을 순회하면서 검색
        for(int i=0; i<blockSum; i++){
            byte[] block = _bufferManager.ReadBlockFromFile(tableName, i);
            for(int j=0; j<blockingFactor; j++){
                byte[] record = _bufferManager.ReadRecordFromBlock(block, recordLength, j);
                if(record != null && !_bufferManager.IsFreeRecord(record)){
                    if(isRecordMatchingPkValue(record, tableName, pkInfoForSearch)){
                        //해당하는 레코드 발견!
                        PrintRecord(record, columnInfo);
                        System.out.println();
                        return;
                    }
                }
            }
        }
        System.out.println("##해당하는 레코드가 없음");
        return;
    }

    /**
     * 특정 테이블의 튜플 한 개 검색하고 block과 record의 인덱스를 반환
     * @param tableName 테이블명
     * @param pkInfoForSearch PK 컬럼정보(key:pk값, value:글자수제한)
     * @return Key: 검색된 레코드가 위치한 Block 인덱스. Value: 해당 Block에서 검색된 레코드의 인덱스<br>
     * 검색된 레코드가 없으면 null 반환
     */
    public LinkedHashMap<Integer,Integer> SearchTupleWithPkNoPrint(String tableName, LinkedHashMap<String, String> pkInfoForSearch) {
        LinkedHashMap<String,String> columnInfo = GetColumnInfo(tableName);
        int recordLength = GetRecordLength(tableName);
        int blockingFactor = _bufferManager.BLOCK_SIZE / recordLength;

        if(columnInfo.isEmpty()) {
            return null;
        }

        // 파일의 block의 총 갯수
        int blockSum = (int)GetFileSize(tableName) / _bufferManager.BLOCK_SIZE;

        // 검색값(pk값)에 해당하는 record가 나올때까지 block을 순회하면서 검색
        for(int i=0; i<blockSum; i++){
            byte[] block = _bufferManager.ReadBlockFromFile(tableName, i);
            for(int j=0; j<blockingFactor; j++){
                byte[] record = _bufferManager.ReadRecordFromBlock(block, recordLength, j);
                if(record != null && !_bufferManager.IsFreeRecord(record)){
                    if(isRecordMatchingPkValue(record, tableName, pkInfoForSearch)){
                        //해당하는 레코드 발견!  
                        LinkedHashMap<Integer,Integer> recordInfo = new LinkedHashMap<>();
                        recordInfo.put(i, j);
                        return recordInfo;
                    }
                }
            }
        }
        return null;
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
     * 테이블의 PK 컬럼명을 출력
     * @param tableName 테이블명
     * @return PK 컬럼메타데이터가 있어서 정상출력이면 true, 없으면 false
     */
    public boolean PrintPkColumnNames(String tableName) {
        List<String> condition = new ArrayList<>();
        condition.add("relation_name = " +  "'" + tableName +"'");
        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_attributeMetaTable,condition, "position ASC");
        int rows = rl.size();
        
        if(rl.isEmpty()) {
            System.out.println("##해당 테이블에대한 컬럼 메타데이터가 없음");
            return false;
        } else {
            System.out.println("[ " + tableName + " 테이블의 PK 컬럼 목록 ]");
            for (int i = 0; i < rows; i++) {
                if(rl.get(i).get("is_PK").equals("1")){ //PK 컬럼만 가져옴
                    System.out.println(" -> 컬럼명: " + rl.get(i).get("attribute_name") + ", 글자수 제한: " + rl.get(i).get("length"));
                }
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
     * 테이블의 PK 컬럼정보(key:컬럼명, value:글자수 제한)를 가져오는 메소드
     * @param tableName 테이블명
     * @return 순서가 보장되는 PK 컬럼정보 맵. 메타데이터에 없으면 빈 맵 반환
     */
    public LinkedHashMap<String,String> GetPkColumnInfo(String tableName) {
        List<String> condition = new ArrayList<>();
        condition.add("relation_name = " +  "'" + tableName +"'");
        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_attributeMetaTable,condition, "position ASC");
        int rows = rl.size();

        LinkedHashMap<String,String> pkColumnInfo = new LinkedHashMap<>();

        if(rl.isEmpty()) {
            return pkColumnInfo;
        } else {
            for (int i = 0; i < rows; i++) {
                if(rl.get(i).get("is_PK").equals("1")){ //PK 컬럼만 가져옴
                    pkColumnInfo.put(rl.get(i).get("attribute_name"), rl.get(i).get("length"));
                }
            }
            return pkColumnInfo;
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
    
    /**
     * 레코드를 출력하는 메소드
     * @param record 레코드 바이트 배열
     * @param columnInfo 컬럼정보(key:컬럼명, value:글자수 제한)
     */
    public void PrintRecord(byte[] record, LinkedHashMap<String,String> columnInfo) {
        int start = 0;
        for (int k = 0; k < columnInfo.size(); k++) {
            int currColLen = Integer.parseInt(columnInfo.values().toArray()[k].toString());
            String str = new String();
            
            for(int i = 0; i<currColLen; i++){
                if(record[start+i] == (byte) 0){ //공백문자처리
                    str += " ";
                }
                else{
                    str += Character.toString((char)record[start+i]);
                }
            }
            System.out.printf(str+ "  |  ");
            start += currColLen;
        }
    }
    
    /**
     * 레코드가 PK값과 일치하는지 확인하는 메소드
     * @param record 레코드 바이트 배열
     * @param tableName 테이블명
     * @param pkInfoForSearch 검색하려는 PK값 정보(key:pk값, value:글자수제한)
     * @return PK값이 일치하면 true, 일치하지 않으면 false
     */
    public boolean isRecordMatchingPkValue(byte[] record, String tableName, LinkedHashMap<String, String> pkInfoForSearch) {
        LinkedHashMap<String, String> columnInfo = GetColumnInfo(tableName); //컬럼정보(key:컬럼명, value:글자수 제한)
        LinkedHashMap<String, String> pkColumnInfo = GetPkColumnInfo(tableName); //PK컬럼정보(key:컬럼명, value:글자수 제한)

        int start = 0;
        boolean valid = true;
        int count = 0;
        
        for (int k = 0; k < columnInfo.size(); k++) {
            //columninfo의 k번째 key값
            String currColName = columnInfo.keySet().toArray()[k].toString();
            int currColLen = Integer.parseInt(columnInfo.values().toArray()[k].toString());
            
            //pkColumnInfo의 key값들중에 currColName이 있으면 ( = 즉 pk컬럼)
            if(pkColumnInfo.containsKey(currColName)) {
                //record바이트배열에서 start부터 currColLen만큼의 바이트를 가져와서 pkInfoForSearch의 count번째 key값과 비교
                byte[] columnBytes = Arrays.copyOfRange(record, start, start + currColLen);
                String pkValue = new String(columnBytes, StandardCharsets.UTF_8).trim();
                String serchingPkValue = (String) pkInfoForSearch.keySet().toArray()[count];
                
                if(!pkValue.equals(serchingPkValue)) {
                    return false;
                }
                else {
                    count++;
                }
                
            }
            

            start += currColLen;
        }
        
        if(valid && count == pkInfoForSearch.size()) {
            return true;
        } else {
            return false;
        }
        
    }
        
}

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
    private static final String TEMP_FILE_PATH = "tempFiles/"; //임시 파일들이 저장될 경로
    private static final String _relationMetaTable = "relation_metadata"; //테이블 메타데이터를 저장하는 테이블 이름
    private static final String _attributeMetaTable = "attribute_metadata"; //컬럼 메타데이터를 저장하는 테이블 이름
    
    private static final int _partitionNum = 3; //파티션 수
    
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

    public void InsertTuple(String tableName, List<Pair<String,String>> newColumnValueInfo) {
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
     * @param pkInfoForDeletion PK 컬럼정보 리스트(first:pk값, second:글자수제한)
     */
    public void DeleteTuple(String tableName, List<Pair<String,String>> pkInfoForDeletion) {
        
        //Key: 검색된 레코드가 위치한 Block 인덱스. Value: 해당 Block에서 검색된 레코드의 인덱스
        //검색된 레코드가 없으면 null
        Pair<Integer,Integer> recordInfo = SearchTupleWithPkNoPrint(tableName, pkInfoForDeletion);
        
        if(recordInfo==null) {
            System.out.println("##삭제하고자 하는 레코드가 없음");
            return;
        }
        
        int blockIndex = recordInfo.first;
        int recordIndex = recordInfo.second;
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
     * @param pkInfoForSearch PK 컬럼정보 리스트(first:pk값, second:글자수제한)
     */
    public void SearchTupleWithPk(String tableName, List<Pair<String,String>> pkInfoForSearch) {
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
     * @param pkInfoForSearch PK 컬럼정보 리스트(first:pk값, second:글자수제한)
     * @return first: 검색된 레코드가 위치한 Block 인덱스. second: 해당 Block에서 검색된 레코드의 인덱스<br>
     * 검색된 레코드가 없으면 null 반환
     */
    public Pair<Integer,Integer> SearchTupleWithPkNoPrint(String tableName, List<Pair<String,String>> pkInfoForSearch) {
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
                        Pair<Integer,Integer> recordInfo = new Pair<>(i,j);
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
     * 실제파일+메타데이터가 존재하는 테이블의 갯수를 가져오는 메소드
     * @return 테이블의 갯수
     */
    public int GetTableNum() {
        int count=0;
        
        List<LinkedHashMap<String,String>> rl = _metaDataManager.SearchTable(_relationMetaTable, null, null);
        if(rl.isEmpty()){
            return 0;
        } else {
            for (int i = 0; i < rl.size(); i++) {
                //메타데이터 검색 결과리스트에서, 현재 row의 relation_name을 가져옴
                String tableName = rl.get(i).get("relation_name");

                //실제 파일이 존재해야지만 출력가능
                File file = new File(FILE_PATH + tableName);
                if (file.exists()) {
                    count++;
                }
            }
        }
        
        return count;
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
     * @param pkInfoForSearch 검색하려는 PK값 정보 리스트(first:pk값, second:글자수제한)
     * @return PK값이 일치하면 true, 일치하지 않으면 false
     */
    public boolean isRecordMatchingPkValue(byte[] record, String tableName, List<Pair<String,String>> pkInfoForSearch) {
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
                String serchingPkValue = (String) pkInfoForSearch.get(count).getFirst();
                
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

    /**
     * 2개 테이블의 해시 조인 수행 및 결과 출력
     * @param tableColumnNamePairs 테이블과 컬럼명 쌍 리스트(2개)
     */
    public void HashEquiJoin(List<Pair<String, String>> tableColumnNamePairs) {
        
        String tableName1 = tableColumnNamePairs.get(0).getFirst();
        String columnName1 = tableColumnNamePairs.get(0).getSecond();
        String tableName2 = tableColumnNamePairs.get(1).getFirst();
        String columnName2 = tableColumnNamePairs.get(1).getSecond();

        int recordLength1 = GetRecordLength(tableName1);
        int blockingFactor1 = _bufferManager.BLOCK_SIZE / recordLength1;
        
        int recordLength2 = GetRecordLength(tableName2);
        int blockingFactor2 = _bufferManager.BLOCK_SIZE / recordLength2;
        
        List<byte[]> result = new ArrayList<>() ; //조인 결과를 저장할 바이트배열 리스트
        
        //파티션 과정 (임시 파일 tempFiles폴더 산하에 '테이블명-part#' 형식으로 생성)
        Partitioning(tableName1, columnName1);
        Partitioning(tableName2, columnName2);
        
        
        
        //파티션된 임시 파일들을 읽어서 비교하는 과정
        //build input으로 사용할 파일들의 이름을 가져옴
        List<String> buildInputNames = _bufferManager.GetFilesWithPrefix(TEMP_FILE_PATH, tableName1+"-part");
        //probe input으로 사용할 파일들의 이름을 가져옴
        List<String> probeInputNames = _bufferManager.GetFilesWithPrefix(TEMP_FILE_PATH, tableName2+"-part");


        //probeInputNames에서 -part 뒤에 있는 숫자목록을 가져옴 
        List<Integer> probeInputNums = new ArrayList<>();
        for(String probeInputName : probeInputNames){
            int probeInputNum = Integer.parseInt(probeInputName.substring(probeInputName.lastIndexOf("-part")+5));
            probeInputNums.add(probeInputNum);
        }
        
        for(String buildInputName : buildInputNames){
            //buildInputName에서 -part 뒤에 있는 숫자를 가져옴
            int buildInputNum = Integer.parseInt(buildInputName.substring(buildInputName.lastIndexOf("-part")+5));
            
            //현재 buildInput의 파티션과 대응되는 probeInput의 파티션이 없다면 패스
            if(!probeInputNums.contains(buildInputNum)){
                continue;
            }
            
            //HashMap을 사용해서 인메모리 hash index를 구현함(key: join컬럼값, value: 레코드 바이트배열 리스트(동일 컬럼값을 지닌 레코드가 복수개가 가능하기 때문))
            Map<String,List<byte[]>> hashIndex = new HashMap<>();
            
            int buildInputBlockNum = (int)GetTempFileSize(buildInputName) / _bufferManager.BLOCK_SIZE; //buildInputName의 block의 갯수를 구함

            
            
            //build input의 모든 블록들을 루프를 돌면서 hash index를 만드는 과정
            for(int i=0; i<buildInputBlockNum; i++) {
                byte[] buildInputBlock = _bufferManager.ReadBlockFromTempFile(buildInputName, i);
                
                for(int j=0; j<blockingFactor1; j++) {
                    byte[] record = _bufferManager.ReadRecordFromBlock(buildInputBlock, GetRecordLength(tableName1), j);
                    
                    if(record != null && !_bufferManager.IsFreeRecord(record)){
                        //record바이트배열에서 join컬럼값을 가져와서 해시함수에 넣는 로직
                        String columnValue = GetColumnValueFromRecord(record, columnName1, tableName1);
                        
                        //hash index에 columnValue 어떤것을 put했는지 출력 
                        System.out.println(buildInputName+"의 인메모리 hash인덱스에 " + columnValue + "를 넣었음.");
                        
                        //hash index에 columnValue를 key로, record를 value로 list에 추가
                        if(!hashIndex.containsKey(columnValue)){
                            List<byte[]> recordList = new ArrayList<>();
                            recordList.add(record);
                            hashIndex.put(columnValue, recordList);
                        } else {
                            hashIndex.get(columnValue).add(record);
                        }
                    }
                }
            }
             
            //probe input의 모든 블록들을 루프를 돌면서 hash index를 이용해서 조인하는 과정
            int probeInputBlockNum = (int)GetTempFileSize(tableName2+"-part"+buildInputNum) / _bufferManager.BLOCK_SIZE; //probeInputName의 block의 갯수를 구함
            
            for(int i=0; i<probeInputBlockNum; i++) {
                byte[] probeInputBlock = _bufferManager.ReadBlockFromTempFile(tableName2 + "-part" + buildInputNum, i);

                for (int j = 0; j < blockingFactor2; j++) {
                    byte[] probeInputrecord = _bufferManager.ReadRecordFromBlock(probeInputBlock, GetRecordLength(tableName2), j);

                    if (probeInputrecord != null && !_bufferManager.IsFreeRecord(probeInputrecord)) {
                        //hash index를 이용해서 equi-join짝이 있는지 검증하는 부분
                        String probeInputColumnValue = GetColumnValueFromRecord(probeInputrecord, columnName2, tableName2);
                        
                        if(hashIndex.containsKey(probeInputColumnValue)) {
                            for(int k=0; k<hashIndex.get(probeInputColumnValue).size(); k++) {
                                //hash index에 동일 join컬럼값이 존재하면 조인한 결과를 저장
                                byte[] buildInputRecord = hashIndex.get(probeInputColumnValue).get(k);

                                //result 바이트배열리스트에 buildInputRecord바이트배열과 probeInputrecord바이트배열을 합친것을 add함
                                byte[] joinedRecord = new byte[buildInputRecord.length + probeInputrecord.length];
                                System.arraycopy(buildInputRecord, 0, joinedRecord, 0, buildInputRecord.length);
                                System.arraycopy(probeInputrecord, 0, joinedRecord, buildInputRecord.length, probeInputrecord.length);
                                result.add(joinedRecord);
                            }
                        }
                    }
                }
            }
        }
        
        //join된 최종 결과를 출력
        PrintHashJoinResult(result, tableName1, tableName2);
    }

    /**
     * hash함수를 적용하여서 파티션을 나누는어 임시파일을 생성하는 메소드
     * @param tableName 파티셔닝할 테이블명
     * @param columnName 해시함수에 사용할 컬럼명
     */
    private void Partitioning(String tableName, String columnName) {
        //해당 테이블명으로 존재하던 임시파일을 모두 지움
        DeleteTempFiles(tableName);
        
        int recordLength = GetRecordLength(tableName);
        int blockingFactor = _bufferManager.BLOCK_SIZE / recordLength;
        LinkedHashMap<String,String> columnInfo = GetColumnInfo(tableName); //key:컬럼명, value:글자수
        int startIndex = 0;
        int hashColumnLength = 0;
        for (Map.Entry<String, String> entry : columnInfo.entrySet()) {
            if (entry.getKey().equals(columnName)) {
                hashColumnLength = Integer.parseInt(entry.getValue());
                break;
            }
            startIndex += Integer.parseInt(entry.getValue());
        }
        
        //140바이트짜리 바이트배열(버켓) 3개 리스트. 각각의 버켓에 파티션된 레코드들을 저장
        List<Pair<byte[],Integer>> buckets = new ArrayList<>(); //pair의 key: 바이트배열, value: 현재 몇 바이트 저장되었는지
        for(int i=0; i<_partitionNum; i++){
            Pair<byte[],Integer> bucket = new Pair<>(new byte[_bufferManager.BLOCK_SIZE], 0);
            buckets.add(bucket);
        }
        

        //파일의 block의 총 갯수
        int blockSum = (int)GetFileSize(tableName) / _bufferManager.BLOCK_SIZE;
        
        // 파일의 block의 총 갯수만큼 반복하여서 파티션을 나눔 (임시파일 tempFiles폴더 산하에 '테이블명-part#' 형식으로 생성)
        for(int i=0; i<blockSum; i++){
            byte[] block = _bufferManager.ReadBlockFromFile(tableName, i); //파일에서 block을 읽어옴
            for(int j=0; j<blockingFactor; j++){
                byte[] record = _bufferManager.ReadRecordFromBlock(block, recordLength, j); //block에서 레코드를 읽어옴
                if(record != null && !_bufferManager.IsFreeRecord(record)){
                    //레코드의 컬럼값을 해시함수에 넣어서 파티션을 나누는 로직
                    //record바이트배열에서 startIndex부터 hashColumnLength만큼의 바이트를 가져와서 해시함수에 넣음
                    byte[] hashColumnBytes = Arrays.copyOfRange(record, startIndex, startIndex + hashColumnLength);
                    int bucketNum = Math.abs(Arrays.hashCode(hashColumnBytes)) % _partitionNum;
                    
                    //record 바이트 배열을 bucketNum에 해당하는 bucket에다가 저장
                    Pair<byte[],Integer> bucket = buckets.get(bucketNum); //pair의 key: 바이트배열, value: 현재 몇 바이트 저장되었는지
                    //record 바이트배열을 bucket에다가 넣으면 넘치게될때, bucket의 내용을 임시파일에다가 이어붙이고 bucket을 비움
                    if(bucket.getSecond()+recordLength > _bufferManager.BLOCK_SIZE){
                        byte[] bytesToAppend = bucket.getFirst();
                        String filePath = TEMP_FILE_PATH+ tableName + "-part" + bucketNum;
                        _bufferManager.AppendBytesToFile(bytesToAppend, filePath);
                        
                        bucket.first = new byte[_bufferManager.BLOCK_SIZE];
                        bucket.second = 0;
                    }
                    
                    int currentSavedBytesNum = bucket.getSecond();
                    for (int k = 0; k < recordLength; k++) {
                        bucket.getFirst()[currentSavedBytesNum + k] = record[k]; //bucket의 바이트배열에 currentSavedBytesNum부터 record바이트배열을 저장
                    }
                    bucket.second = currentSavedBytesNum + recordLength;
                    
                }
            }
            System.out.print(tableName+ "테이블의 " + i + "번째 블록 파티셔닝 완료\n");
        }
        
        //마지막으로 남은 bucket의 내용을 bucket이 빈 바이트배열이 아닐때에만 임시파일에다가 저장
        for(int i=0; i<_partitionNum; i++){
            Pair<byte[],Integer> bucket = buckets.get(i);
            if(bucket.getSecond() > 0){
                byte[] bytesToAppend = bucket.getFirst();
                String filePath = TEMP_FILE_PATH+ tableName + "-part" + i;
                _bufferManager.AppendBytesToFile(bytesToAppend, filePath);
            }
        }
        
    }

    /**
     * tableName-part# 형식의 임시파일을 모두 삭제하는 메소드
     * @param tableName
     */
    public void DeleteTempFiles(String tableName) {
        String prefix = tableName + "-part";
        File tempFilesDir = new File("tempFiles");

        // Get all files in the directory
        File[] files = tempFilesDir.listFiles();

        if (files != null) {
            for (File file : files) {
                // If the file name starts with the prefix, delete it
                if (file.getName().startsWith(prefix)) {
                    boolean deleted = file.delete();
                    if (!deleted) {
                        System.out.println("Failed to delete file: " + file.getName());
                    }
                }
            }
        }
    }
    
    public long GetTempFileSize(String tableName) {
        File file = new File(TEMP_FILE_PATH + tableName);
        return (long)file.length();
    }
    
    /**
     * 레코드에서 컬럼값을 가져오는 메소드
     * @param record 레코드 바이트 배열
     * @param columnName 값을 구하고자 하는 컬럼명
     * @param tableName 테이블명
     * @return 컬럼값(String)
     */
    public String GetColumnValueFromRecord(byte[] record, String columnName, String tableName) {
        LinkedHashMap<String,String> columnInfo = GetColumnInfo(tableName);
        int startIndex = 0;
        for (Map.Entry<String, String> entry : columnInfo.entrySet()) {
            if (entry.getKey().equals(columnName)) {
                int columnLength = Integer.parseInt(entry.getValue());
                byte[] columnBytes = Arrays.copyOfRange(record, startIndex, startIndex + columnLength);
                String columnValue = new String(columnBytes, StandardCharsets.UTF_8).trim();
                return columnValue;
            }
            startIndex += Integer.parseInt(entry.getValue());
        }
        return null;
    }
    
    /**
     * 해시 조인 결과를 출력하는 메소드
     * @param result 조인 결과 바이트배열 리스트
     * @param tableName1 조인할 테이블명1
     * @param tableName2 조인할 테이블명2
     */
    public void PrintHashJoinResult(List<byte[]> result, String tableName1, String tableName2) {

        //조인된 결과의 레코드 갯수 출력
        System.out.println("\n조인된 결과의 레코드 갯수: " + result.size());
        
        LinkedHashMap<String,String> columnInfo1 = GetColumnInfo(tableName1);
        LinkedHashMap<String,String> columnInfo2 = GetColumnInfo(tableName2);
        
        //컬럼명 순서대로 출력
        for (Map.Entry<String, String> entry : columnInfo1.entrySet()) {
            System.out.print(entry.getKey() + "  |  ");
        }
        for (Map.Entry<String, String> entry : columnInfo2.entrySet()) {
            System.out.print(entry.getKey() + "  |  ");
        }
        System.out.println();
        
        
        for(byte[] record : result){
            //첫번째 테이블의 레코드 출력
            PrintRecord(record, columnInfo1);
            //두번째 테이블의 레코드 출력
            PrintRecord(Arrays.copyOfRange(record, GetRecordLength(tableName1), record.length), columnInfo2);
            System.out.println();
        }
    }
}

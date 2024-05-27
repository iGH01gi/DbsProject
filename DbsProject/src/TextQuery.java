import java.lang.annotation.ElementType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 사용자 입력을 최전선에서 받는 클래스
 */
public class TextQuery {
    InputValidator _inputValidator; // 사용자 입력 유효성 검사를 위한 인스턴스
    QueryEvaluationEngine _queryEvaluationEngine; // 실질적인 쿼리 처리를 위한 인스턴스
    
    public TextQuery() {
        _inputValidator = new InputValidator();
        _queryEvaluationEngine = new QueryEvaluationEngine();
    }
    
    public void Start() {
        while (true) {
            PrintMenu();
            int command = _inputValidator.GetMenuInput();
            if (command == 0) {
                System.out.println("프로그램을 종료");
                System.exit(0);
                break;
            }
            switch (command) {
                case 1: {
                    String tableName = Process1_1();
                    Map<String, Integer> columns = Process1_2();
                    List<String> pkColumns = Process1_3(columns);
                    //여기까지 모든 입력이 정상적이면, 실제 테이블 생성 및 메타데이터 생성.
                    _queryEvaluationEngine.CreateTable(tableName, columns, pkColumns);
                    break;
                }
                case 2: {
                    String tableName = Process2_1();
                    if(tableName == null){ 
                        break; //테이블이 아무것도 존재하지 않을 경우
                    }
                    List<Pair<String,String>> newColumnValueInfo = Process2_2(tableName);
                    if(newColumnValueInfo.isEmpty()) {
                        break; //컬럼 메타데이터가 없는 비정상적인 상황
                    }
                    //여기까지 모든 입력이 정상적이면, 실제 튜플 삽입
                    _queryEvaluationEngine.InsertTuple(tableName, newColumnValueInfo);
                    break;
                }
                case 3:{
                    String tableName = Process3_1();
                    if(tableName == null){ 
                        break; //테이블이 아무것도 존재하지 않을 경우
                    }
                    List<Pair<String,String>> pkInfoForDeletion= Process3_2(tableName);
                    if(pkInfoForDeletion.isEmpty()){
                        break; //컬럼 메타데이터가 없는 비정상적인 상황
                    }
                    _queryEvaluationEngine.DeleteTuple(tableName, pkInfoForDeletion);
                    break;
                }
                case 4: {
                    String tableName = Process4_1();
                    if (tableName == null) {
                        break; //테이블이 아무것도 존재하지 않을 경우
                    }
                    _queryEvaluationEngine.SearchTable(tableName);
                    break;
                }
                case 5:{
                    String tableName = Process5_1();
                    if (tableName == null) {
                        break; //테이블이 아무것도 존재하지 않을 경우
                    }
                    List<Pair<String,String>> pkInfoForSearch = Process5_2(tableName);
                    if(pkInfoForSearch.isEmpty()){
                        break; //컬럼 메타데이터가 없는 비정상적인 상황
                    }
                    _queryEvaluationEngine.SearchTupleWithPk(tableName, pkInfoForSearch);
                    break;
                }
                case 6:{
                    List<String>tableNames = Process6_1(); 
                    if (tableNames == null) {
                        break; //테이블이 2개 이상 존재하지 않을 경우
                    }
                    List<Pair<String,String>> tableColumnNamePairs = Process6_2(tableNames); //Pair의 key: 테이블명, value: 컬럼명
                    if(tableColumnNamePairs.isEmpty()){
                        break; //컬럼 메타데이터가 없는 비정상적인 상황
                    }
                    
                    //여기까지 모든 입력이 정상적이면, 실제 Hash-Equi-Join 연산 수행
                    _queryEvaluationEngine.HashEquiJoin(tableColumnNamePairs);
                    break;
                }
                default:
                    System.out.println("잘못된 입력");
                    break;  
            }
        }
    }
    


    /**
     * 사용자에게 메뉴를 출력하는 함수
     */
    private void PrintMenu() {
        System.out.println("\n\n------------------------------------------------------------------------------------------------------");
        System.out.println("0. 종료 1. 테이블 생성 2. 튜플 삽입 3. 튜플 삭제 4. 특정 테이블의 전체 튜플 검색 5.특정 테이블의 튜플 한 개 검색 6.Hash-Equi-Join 연산");
        System.out.print("원하는 기능의 번호를 입력: ");
    }
    
    //region '1. 테이블 생성' 관련 print 및 input처리 함수
    
    private String Process1_1(){
        System.out.print("\n생성할 테이블의 이름을 입력(30글자 이내): ");
        return _inputValidator.Get1_1Input();
    }
    
    private Map<String, Integer> Process1_2(){
        return _inputValidator.Get1_2Input();
    }
    
    private List<String> Process1_3(Map<String, Integer> columns){
        return _inputValidator.Get1_3Input(columns);
    }

    //endregion
    
    //region '2. 튜플 삽입' 관련 print 및 input처리 함수
    
    private String Process2_1(){
        while(true) {
            //현재 존재하는 테이블명들을 출력하고 true리턴. 없을시 false 리턴
            if (_queryEvaluationEngine.PrintAllTableNames() == false){
                return null;
            }
            
            //튜플을 삽입할 테이블명 입력받음
            String tableName = _inputValidator.Get2_1Input();
            
            //테이블이 존재하는지 확인
            if (_queryEvaluationEngine.IsTableExist(tableName)) {
                return tableName;
            }
            else {
                System.out.println("올바른 테이블명을 다시 입력!!\n");
            }
        }
    }
    
    private List<Pair<String,String>> Process2_2(String tableName){
        //테이블의 컬럼 정보를 메타데이터로부터 가져와서 출력 및 저장
        LinkedHashMap<String,String> columnInfo = new LinkedHashMap(); //key: 컬럼이름, value: 글자수 제한
        if(!_queryEvaluationEngine.PrintColumnNames(tableName)){ //컬럼 메타데이터가 없는 비정상적인 상황
            return new ArrayList<>();
        }
        else{
            columnInfo = _queryEvaluationEngine.GetColumnInfo(tableName);
            if(columnInfo.isEmpty()){ //컬럼 메타데이터가 없는 비정상적인 상황
                return new ArrayList<>();
            }
        }
        
        //튜플 삽입할 값들 ,로 구분해서 입력받음 (글자수 제한 지키도록 제한하고 있음)
        List<Pair<String,String>> newColumnValues = _inputValidator.Get2_2Input(columnInfo);
        
        return newColumnValues;
    }
    //endregion
    
    //region '3. 튜플 삭제' 관련 print 및 input처리 함수
    
    private String Process3_1(){
        while(true) {
            //현재 존재하는 테이블명들을 출력하고 true리턴. 없을시 false 리턴
            if (_queryEvaluationEngine.PrintAllTableNames() == false){
                return null;
            }

            //튜플을 삽입할 테이블명 입력받음
            String tableName = _inputValidator.Get3_1Input();

            //테이블이 존재하는지 확인
            if (_queryEvaluationEngine.IsTableExist(tableName)) {
                return tableName;
            }
            else {
                System.out.println("올바른 테이블명을 다시 입력!!\n");
            }
        }
    } 
    
    /**
     * 튜플 삭제를 위한 입력을 받는 함수
     * @param tableName 삭제하고자하는 튜플이 있는 테이블명
     * @return 삭제할 튜플의 컬럼명과 값
     */
    private List<Pair<String,String>> Process3_2(String tableName){
        //테이블의 컬럼 정보를 메타데이터로부터 가져와서 출력 및 저장
        LinkedHashMap<String,String> columnInfo = new LinkedHashMap(); //key: 컬럼이름, value: 글자수 제한
        if(!_queryEvaluationEngine.PrintPkColumnNames(tableName)){ //컬럼 메타데이터가 없는 비정상적인 상황
            return new ArrayList<>();
        }
        else{
            columnInfo = _queryEvaluationEngine.GetPkColumnInfo(tableName);
            if(columnInfo.isEmpty()){ //컬럼 메타데이터가 없는 비정상적인 상황
                return new ArrayList<>();
            }
        }

        //튜플 삭제할 값들 ,로 구분해서 입력받음 (글자수 제한 지키도록 제한하고 있음)
        List<Pair<String,String>> pkInfoForDeletion = _inputValidator.Get3_2Input(columnInfo);

        return pkInfoForDeletion;
    }
    
    //endregion
    
    //region '4. 테이블 검색' 관련 print 및 input처리 함수
    
    private String Process4_1(){
        while(true) {
            //현재 존재하는 테이블명들을 출력하고 true리턴. 없을시 false 리턴
            if (_queryEvaluationEngine.PrintAllTableNames() == false){
                return null;
            }
            
            //검색할 테이블명을 입력받음
            String tableName = _inputValidator.Get4_1Input();
            
            //테이블이 존재하는지 확인
            if (_queryEvaluationEngine.IsTableExist(tableName)) {
                return tableName;
            }
            else {
                System.out.println("올바른 테이블명을 다시 입력!!\n");
            }
        }
    }
    
    //endregion
    
    //region '5. pk로 튜플 검색' 관련 print 및 input처리 함수
    private String Process5_1(){
        while(true) {
            //현재 존재하는 테이블명들을 출력하고 true리턴. 없을시 false 리턴
            if (_queryEvaluationEngine.PrintAllTableNames() == false){
                return null;
            }

            //튜플을 삽입할 테이블명 입력받음
            String tableName = _inputValidator.Get5_1Input();

            //테이블이 존재하는지 확인
            if (_queryEvaluationEngine.IsTableExist(tableName)) {
                return tableName;
            }
            else {
                System.out.println("올바른 테이블명을 다시 입력!!\n");
            }
        }
    }
    
    private List<Pair<String,String>> Process5_2(String tableName){
        //테이블의 컬럼 정보를 메타데이터로부터 가져와서 출력 및 저장
        LinkedHashMap<String,String> pkColumnInfo = new LinkedHashMap(); //key: 컬럼이름, value: 글자수 제한
        if(!_queryEvaluationEngine.PrintPkColumnNames(tableName)){ //컬럼 메타데이터가 없는 비정상적인 상황
            return new ArrayList<>();
        }
        else{
            pkColumnInfo = _queryEvaluationEngine.GetPkColumnInfo(tableName);
            if(pkColumnInfo.isEmpty()){ //컬럼 메타데이터가 없는 비정상적인 상황
                return new ArrayList<>();
            }
        }

        //튜플 검색할 값들 ,로 구분해서 입력받음 (글자수 제한 지키도록 제한하고 있음)
        List<Pair<String,String>> pkInfoForSearch = _inputValidator.Get5_2Input(pkColumnInfo);

        return pkInfoForSearch;
    }
    
    //endregion

    //region '6. Hash-Equi-Join 연산' 관련 print 및 input처리 함수
    private List<String> Process6_1() {
        while(true) {
            //현재 존재하는 테이블명들을 출력하고 true리턴. 없을시 false 리턴
            if (_queryEvaluationEngine.PrintAllTableNames() == false) {
                return null;
            }

            if (_queryEvaluationEngine.GetTableNum() < 2) {
                System.out.println("테이블이 2개 이상 존재해야 Hash-Equi-Join 연산이 가능.");
                return null;
            }

            //검색할 테이블명 2개를 입력받음
            List<String> tableNames = _inputValidator.Get6_1Input();

            //테이블이 존재하는지 확인
            if (_queryEvaluationEngine.IsTableExist(tableNames.get(0)) && _queryEvaluationEngine.IsTableExist(tableNames.get(1))) {
                return tableNames;
            }
            else {
                System.out.println("올바른 테이블명을 다시 입력!!\n");
            }
        }
    }
    
    //Pair의 key: 테이블명, value: 컬럼명
    private List<Pair<String,String>> Process6_2(List<String> tableNames){
        //테이블의 컬럼 정보를 메타데이터로부터 가져와서 출력
        LinkedHashMap<String,String> columnInfo1 = new LinkedHashMap(); //key: 컬럼이름, value: 글자수 제한
        LinkedHashMap<String,String> columnInfo2 = new LinkedHashMap(); //key: 컬럼이름, value: 글자수 제한
        
        if(!_queryEvaluationEngine.PrintColumnNames(tableNames.get(0)) || !_queryEvaluationEngine.PrintColumnNames(tableNames.get(1))){ //컬럼 메타데이터가 없는 비정상적인 상황
            return new ArrayList<>();
        }
        else{
            columnInfo1 = _queryEvaluationEngine.GetColumnInfo(tableNames.get(0));
            columnInfo2 = _queryEvaluationEngine.GetColumnInfo(tableNames.get(1));
            if(columnInfo1.isEmpty() || columnInfo2.isEmpty()){ //컬럼 메타데이터가 없는 비정상적인 상황
                return new ArrayList<>();
            }
        }
        
        //hash equi join에 사용할 첫번째 테이블의 컬럼명을 입력받음
        String columnName1 = _inputValidator.Get6_2Input(tableNames.get(0), columnInfo1);
        
        //hash equi join에 사용할 두번째 테이블의 컬럼명을 입력받음
        String columnName2 = _inputValidator.Get6_2Input(tableNames.get(1), columnInfo2);
        
        List<Pair<String,String>> tableColumnNamePairs = new ArrayList<>();
        tableColumnNamePairs.add(new Pair<>(tableNames.get(0), columnName1));
        tableColumnNamePairs.add(new Pair<>(tableNames.get(1), columnName2));
        
        return tableColumnNamePairs;
    }

    //endregion
}


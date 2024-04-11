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
                    List<String> newColumnValues = Process2_2(tableName);
                    if(newColumnValues.isEmpty()) {
                        break; //컬럼 메타데이터가 없는 비정상적인 상황
                    }
                    //여기까지 모든 입력이 정상적이면, 실제 튜플 삽입
                    _queryEvaluationEngine.InsertTuple(tableName, newColumnValues);
                    break;
                }
                case 3:
                    _queryEvaluationEngine.DeleteTuple();
                    break;
                case 4:
                    String tableName = Process4_1();
                    if(tableName == null){ 
                        break; //테이블이 아무것도 존재하지 않을 경우
                    }
                    _queryEvaluationEngine.SearchTable(tableName);
                    break;
                case 5:
                    _queryEvaluationEngine.SearchTupleWithPk();
                    break;
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
        System.out.println("0. 종료 1. 테이블 생성 2. 튜플 삽입 3. 튜플 삭제 4. 특정 테이블의 전체 튜플 검색 5.특정 테이블의 튜플 한 개 검색");
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
    
    private List<String> Process2_2(String tableName){
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
        List<String> newColumnValues = _inputValidator.Get2_2Input(columnInfo);
        
        return newColumnValues;
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
    
}

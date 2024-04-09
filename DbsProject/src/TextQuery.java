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
                case 1:
                    String tableName = Process1_1();
                    Map<String,Integer> columns= Process1_2();
                    List<String> pkColumns= Process1_3(columns);
                    //여기까지 모든 입력이 정상적이면, 실제 테이블 생성 및 메타데이터 생성.
                    _queryEvaluationEngine.CreateTable();
                    break;
                case 2:
                    _queryEvaluationEngine.InsertTuple();
                    break;
                case 3:
                    _queryEvaluationEngine.DeleteTuple();
                    break;
                case 4:
                    _queryEvaluationEngine.SearchTable();
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
        System.out.println("\n\n0. 종료 1. 테이블 생성 2. 튜플 삽입 3. 튜플 삭제 4. 특정 테이블의 전체 튜플 검색 5.특정 테이블의 튜플 한 개 검색");
        System.out.print("원하는 기능의 번호를 입력: ");
    }
    
    //region '1. 테이블 생성' 관련 print 및 input처리 함수
    
    private String Process1_1(){
        System.out.print("생성할 테이블의 이름을 입력(30글자 이내): ");
        return _inputValidator.Get1_1Input();
    }
    
    private Map<String, Integer> Process1_2(){
        return _inputValidator.Get1_2Input();
    }
    
    private List<String> Process1_3(Map<String, Integer> columns){
        return _inputValidator.Get1_3Input(columns);
    }

    //endregion
    
    
    
}

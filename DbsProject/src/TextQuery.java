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
                    Process1_1();
                    Process1_2();
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
    private void Process1_1(){
        System.out.print("생성할 테이블의 이름을 입력(30 바이트 이내): ");
        _inputValidator.Get1_1Input();
    }
    
    private void Process1_2(){
        System.out.print("컬럼 이름을 입력(30 바이트 이내) or 컬럼 입력이 끝났다면 0을 입력: ");
    }
    //endregion
    
    
    
}

import java.util.Scanner;

public class InputValidator {
    /**
     * 사용자로부터 메뉴(0~5) 입력을 받는 함수
     * @return 사용자가 입력한 메뉴 번호.<br> 
     * 잘못된 입력일경우 -1을 반환
     */
    public int GetMenuInput() {
        Scanner scanner = new Scanner(System.in);
        int input;

        try {
            input = scanner.nextInt();
        } catch (Exception e) {
            return -1;
        }

        if (input < 0 || input > 5) {
            return -1;
        }
        
        return input;
    }

    /**
     * '1. 테이블 생성' 관련 입력을 받는 함수
     */
    public void Get1_1Input() {
    }
}

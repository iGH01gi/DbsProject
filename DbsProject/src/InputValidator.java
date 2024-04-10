import java.util.*;

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
            scanner.nextLine(); // consume the newline
        } catch (Exception e) {
            return -1;
        }

        if (input < 0 || input > 5) {
            return -1;
        }
        
        return input;
    }

    //region '1. 테이블 생성' 관련 입력을 받는 함수들
    public String Get1_1Input() {
        Scanner scanner = new Scanner(System.in);
        String input;
        
        while (true) {
            input = scanner.nextLine();
            
            if (input.length() > 30) {
                System.out.print("30글자 이내로 다시 입력: ");
            } else {
                return input;
            }
        }
    }

    public Map<String, Integer> Get1_2Input() {
        Scanner scanner = new Scanner(System.in);
        String inputColName;
        int inputColByte;
        Map<String, Integer> columns = new LinkedHashMap<>(); //순서를 보장

        while (true) {
            inputColName = null;
            inputColByte = 0;
            
            //columns에 있는 모든 value값의 총 합 (현재까지 입력된 컬럼들의 크기 합)
            int sumBytes = columns.values().stream().mapToInt(Integer::intValue).sum();
            int remainBytes = 140 - sumBytes;
            
            if(remainBytes <= 0) {
                System.out.println("\n###더 이상 컬럼을 입력할 수 없음. 지금까지 입력받은 컬럼정보로 테이블을 생성...###");
                return columns;
            }
            System.out.println("\n");
            if(columns.size() > 0) {
                System.out.println("<<현재까지 입력된 컬럼 정보>>");
                for (Map.Entry<String, Integer> entry : columns.entrySet()) {
                    System.out.println("컬럼 이름: " + entry.getKey() + ", 크기(bytes): " + entry.getValue());
                }
            }
            
            System.out.print("컬럼 이름을 입력(30글자 이내) or 컬럼 입력이 끝났다면 0을 입력: ");
            inputColName = scanner.nextLine();

            if (inputColName.length() > 30) {
                System.out.println("30글자 이내로 입력해야함");
            } else {
                if (inputColName.equals("0")) {
                    if(columns.size() <= 0) {
                        System.out.println("컬럼을 하나 이상 입력해야 함");
                        continue;
                    } else {
                        return columns;
                    }
                } else {
                    //컬럼 이름이 중복되는지 확인
                    if(columns.containsKey(inputColName)) {
                        System.out.println("이미 존재하는 컬럼 이름임.");
                        continue;
                    }
                    
                    System.out.println("- Block의 크기는 140바이트로 설정되어 있음");
                    System.out.println("- 입력받을 컬럼의 크기는 현재 최대 " + remainBytes + "bytes까지 가능함");
                    System.out.print("컬럼의 크기를 입력: ");
                    
                    while (true) {
                        if (!scanner.hasNextInt()) {
                            System.out.print("숫자를 입력. 다시 입력: ");
                            scanner.next(); // discard the non-integer input
                            continue;
                        }
                        
                        inputColByte = scanner.nextInt();
                        scanner.nextLine(); // consume the newline
                        
                        if(inputColByte > remainBytes) {
                            System.out.print("입력한 크기가 남은 크기보다 큼. 다시 입력: ");
                            continue;
                        } else {
                            columns.put(inputColName, inputColByte);
                            break;
                        }
                    }
                    
                }
            }
        }
    }

    //PK로 지정할 컬럼명(들)을 입력받는 함수
    public List<String> Get1_3Input(Map<String, Integer> columns) {
        List<String> pkColumns = new ArrayList<>();
        Scanner scanner = new Scanner(System.in);
        
        while (true) {
            System.out.println("\n");
            System.out.println("<<현재 컬럼 정보>>");
            for (Map.Entry<String, Integer> entry : columns.entrySet()) {
                System.out.println("컬럼 이름: " + entry.getKey() + ", 크기(bytes): " + entry.getValue());
            }

            System.out.print("PK로 지정할 컬럼명을 ,로 구분하여 입력 (1개 이상 가능): ");
            String input = scanner.nextLine();
            String[] columnNames = input.split(",");

            boolean isValid = true;
            for (String columnName : columnNames) {
                if (!columns.containsKey(columnName)) {
                    isValid = false;
                    break;
                }
            }

            if (isValid) {
                pkColumns = Arrays.asList(columnNames);
                break;
            } else {
                System.out.println("입력한 컬럼명 중 존재하지 않는 컬럼이 있음.");
            }
        }
        
        
        
        return pkColumns;
    }
    //endregion
    
    //region '2. 튜플 삽입' 관련 입력을 받는 함수들
    
    public String Get2_1Input() {
        Scanner scanner = new Scanner(System.in);
        String input;
        
        while (true) {
            System.out.print("튜플을 삽입 할 테이블명을 입력(30글자 이내): ");
            input = scanner.nextLine();
            
            if (input.length() > 30) {
                System.out.println("30글자 이내로 다시 입력\n");
            } else {
                return input;
            }
        }
    }
    
    //endregion
   
}

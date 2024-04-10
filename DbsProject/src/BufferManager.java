import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;

public class BufferManager {
    private static final int BLOCK_SIZE = 140; //블록 크기(140 bytes)
    private static final String FILE_PATH = "files/"; //파일들이 저장될 경로

    public void CreateFile(String tableName, Map<String, Integer> columns) {
        File file = new File(FILE_PATH + tableName);
        
        //고정 길이 레코드의 길이(bytes)
        int recordLength = columns.values().stream().mapToInt(Integer::intValue).sum(); 
        
        //파일에 헤더 레코드를 넣기위한 블록 임시 생성 (아직 파일에 저장되지 않음)
        byte[] block = CreateBlock(); //(140 bytes)
        //헤더 레코드에 초기값 1 넣기
        SetHeaderRecord(block, 1, recordLength);

        try {
            if (file.createNewFile()) {
                //헤더 레코드가 담긴 초기 block을 파일에 쓰기
                WriteBlockToFile(file, block, 0);
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating the file.");
            e.printStackTrace();
        }
    }

    /**
     * 파일에 실제 블록을 저장하는 메소드가 아닌, 메모리상에 블록을 임시 생성하는 메소드
     */
    public byte[] CreateBlock() {
        return new byte[BLOCK_SIZE];
    }

    /**
     * 파일에 블록을 쓰는 메소드
     * @param file 파일 객체
     * @param block 블록 바이트 배열
     * @param blockPosition 파일에 쓰일 블록의 위치(0부터 시작)
     */
    public void WriteBlockToFile(File file, byte[] block, int blockPosition) {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(block.length * blockPosition);
            raf.write(block);
            raf.close();
        } catch (IOException e) {
            System.out.println("File에 block을 쓰는데 실패했습니다.");
            e.printStackTrace();
        }
    }

    /**
     * 블록에 레코드를 쓰는 메소드
     * @param block 블록 바이트 배열
     * @param record 고정길이 레코드 바이트 배열
     * @param position 블록에서 해당 레코드가 몇번째 레코드가 될지(0부터 시작)
     * @return
     */
    public boolean WriteRecordToBlock(byte[] block, byte[] record, int position) {
        //레코드가 쓰일 위치가 블록의 범위를 벗어나는지 확인
        
        
        for (int i = 0; i < record.length; i++) {
            block[position + i] = record[i];
        }
        
        return true;
    }
    
    /**
     * 미리 읽어온 파일의 첫번째 블록에서<br>
     * 헤더 레코드의 값을 설정하는 메소드
     * @param block 미리 읽어온 파일의 첫번째 블록
     * @param value 헤더 레코드에 넣을 값(= 다음 free record의 위치)
     * @param recordLength 고정 길이 레코드의 길이(bytes)
     * @return
     */
    public void SetHeaderRecord(byte[] block, int value, int recordLength) {
        //헤더 레코드 생성
        byte[] headerRecord = new byte[recordLength];

        //block에 headerRecord 쓰기
        byte[] intBytes = ByteBuffer.allocate(4).putInt(value).array();
        System.arraycopy(intBytes, 0, block, 0, intBytes.length);
    }
}

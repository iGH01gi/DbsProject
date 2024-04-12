import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BufferManager {
    public static final int BLOCK_SIZE = 140; //블록 크기(140 bytes)
    private static final String FILE_PATH = "files/"; //파일들이 저장될 경로

    public void CreateFile(String tableName, Map<String, Integer> columns) {
        File file = new File(FILE_PATH + tableName);
        
        //고정 길이 레코드의 길이(bytes)
        int recordLength = columns.values().stream().mapToInt(Integer::intValue).sum(); 
        
        //파일에 헤더 레코드를 넣기위한 블록 임시 생성 (아직 파일에 저장되지 않음)
        byte[] firstBlock = CreateBlock(); //(140 bytes)
        //헤더 레코드에 초기값 1 넣기
        SetFreeRecordValue(firstBlock,0, 1, recordLength);

        try {
            if (file.createNewFile()) {
                //헤더 레코드가 담긴 초기 block을 파일에 쓰기
                WriteBlockToFile(file, firstBlock, 0);
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
     * @param blockIndex 파일에서 몇번째 블록에 쓸지(0부터 시작)
     */
    public void WriteBlockToFile(File file, byte[] block, int blockIndex) {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(block.length * blockIndex);
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
     * @param recordLength 레코드의 길이(bytes)
     * @param newColumnValueInfo first: 컬럼값 , second: 해당 컬럼이 레코드에서 차지하는 길이
     * @param recordIndex 블록에서 해당 레코드가 몇번째 레코드가 될지(0부터 시작)
     * @return
     */
    public boolean WriteRecordToBlock(byte[] block, int recordLength, List<Pair<String,String>> newColumnValueInfo, int recordIndex) {
        //레코드 생성
        byte[] record = new byte[recordLength];
        int recordPosition = recordIndex * recordLength;
        if(recordPosition + recordLength > block.length){
            System.out.println("##블록의 범위를 벗어나는 위치에 record를 쓰려고 시도!!");
            return false;
        }
        
        //레코드에 값을 넣기
        List<String> newValues = newColumnValueInfo.stream().map(Pair::getFirst).collect(Collectors.toList());
        List<Integer> newLengths = newColumnValueInfo.stream().map(pair -> Integer.parseInt(pair.getSecond())).collect(Collectors.toList());
        int position = 0;
        for (int i=0; i<newValues.size(); i++) {
            byte[] valueBytes = new byte[newLengths.get(i)];
            System.arraycopy(newValues.get(i).getBytes(StandardCharsets.US_ASCII), 0, valueBytes, 0, newValues.get(i).getBytes(StandardCharsets.US_ASCII).length);
            System.arraycopy(valueBytes, 0, record, position, valueBytes.length);
            position += newLengths.get(i);
        }
        
        //블록에 레코드를 쓰기
        System.arraycopy(record, 0, block, recordPosition, recordLength);
        return true;
    }
    
    /**
     * 블록에서 레코드를 읽어오는 메소드
     * @param block 블록 바이트 배열
     * @param recordLength 레코드의 길이(bytes)
     * @param recordIndex 블록에서 몇번째 레코드를 읽어올지(0부터 시작)
     * @return 읽어온 레코드의 바이트 배열 (실패 또는 free record조차 아닌 빈 레코드면 null)
     */
    public byte[] ReadRecordFromBlock(byte[] block, int recordLength, int recordIndex) {
        byte[] record = new byte[recordLength];
        int recordPosition = recordIndex * recordLength;
        if(recordPosition + recordLength > block.length){
            System.out.println("##블록의 범위를 벗어나는 위치에서 record를 읽으려고 시도!!");
            return null;
        }
        System.arraycopy(block, recordPosition, record, 0, recordLength);
        
        //프리레코드에조차 등록되지 않은 빈 레코드인 경우 null 반환
        if (IntStream.range(0, record.length).allMatch(i -> record[i] == 0)) {
            return null;
        }
        
        return record;
    }
    
    /**
     * 미리 읽어온 블록의 특정 free record에<br>
     * 다음 free record 의 값을 설정하는 메소드
     * @param block 미리 읽어온 파일의 블록
     * @param freeRecordIndex free record의 위치(0부터 시작)
     * @param value free record에 넣을 값(= 다음 free record의 위치)
     * @param recordLength 고정 길이 레코드의 길이(bytes)
     * @return
     */
    public void SetFreeRecordValue(byte[] block,int freeRecordIndex, int value, int recordLength) {
        //프리 레코드 생성
        byte[] freeRecord = new byte[recordLength];

        //프리 레코드에 값을 넣기
        byte[] intBytes = ByteBuffer.allocate(4).putInt(value).array();
        System.arraycopy(intBytes, 0, freeRecord, 0, intBytes.length);
        
        //프리 레코드의 마지막바이트에 soh 넣어서 free-record임을 표시
        freeRecord[recordLength-1] = (byte)1; // SOH in ASCII
        
        //블록에 프리 레코드를 쓰기
        int freeRecordPosition = freeRecordIndex * recordLength;
        if(freeRecordPosition + freeRecord.length > block.length){
            System.out.println("##블록의 범위를 벗어나는 위치에 freeRecord를 쓰려고 시도!!");
            return;
        }
        System.arraycopy(freeRecord, 0, block, freeRecordPosition, freeRecord.length);
    }
    
    /**
     * 미리 읽어온 파일의 블록에서<br>
     * free record의 값을 읽어오는 메소드
     * @param block 미리 읽어온 블록
     * @param freeRecordIndex free record의 위치(0부터 시작)
     * @param recordLength 고정 길이 레코드의 길이(bytes)
     * @return free record에 저장된 값(= 다음 free record의 위치) -1이면 오류
     */
    public int GetFreeRecordValue(byte[] block, int freeRecordIndex, int recordLength) {
        /*if(!(block[recordLength*(freeRecordIndex+1)-1] == (byte)1)){
            System.out.println("##free record이 아닌 곳에서 freeRecord를 읽으려고 시도!!");
            return -1;
        }*/
        
        byte[] intBytes = new byte[4];
        int freeRecordPosition = freeRecordIndex * recordLength;
        if(freeRecordPosition + recordLength > block.length){
            System.out.println("##블록의 범위를 벗어나는 위치에서 freeRecord를 읽으려고 시도!!");
            return -1;
        }
        
        System.arraycopy(block, freeRecordPosition, intBytes, 0, intBytes.length);
        int value = ByteBuffer.wrap(intBytes).getInt();
        return value;
    }
    
    /**
     * 파일로부터 블록을 읽어오는 메소드
     * @param tableName 테이블 이름(=파일명)
     * @param blockIndex 파일에서 몇번째 블록을 읽어올지(0부터 시작)
     * @return 읽어온 블록의 바이트 배열
     */
    public byte[] ReadBlockFromFile(String tableName, int blockIndex) {
        try {
            File file = new File(FILE_PATH + tableName);
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            byte[] block = new byte[BLOCK_SIZE];
            raf.seek(block.length * blockIndex);
            int readBytes= raf.read(block);
            raf.close();
            
            if(readBytes == -1){
                System.out.println("파일의 마지막에서부터 블록을 읽으려고 시도했음!");
            }
            else if(readBytes != BLOCK_SIZE){
                System.out.println("블록이 전부 읽히지 않았음. 읽은 바이트 수: " + readBytes);
                System.out.println("블록 계산식 점검 필요");
            }
            else {
                return block;
            }
            
        } catch (IOException e) {
            if(e instanceof EOFException){
                System.out.println("파일의 범위를 넘어서는 곳부터 읽으려고 시도했습니다.");
            }
            else {
                System.out.println("File에서 block을 읽는데 실패했습니다.");
            }
            e.printStackTrace();
        }
        
        return null;
    }
    
    /**
     * 해당 레코드 바이트배열이 freeRecord인지 확인하는 메소드
     * @param record 레코드 바이트 배열
     * @return freeRecord이면 true, 아니면 false
     */
    public Boolean IsFreeRecord(byte[] record){
        if(record[record.length-1] == (byte)1){
            return true;
        }
        else {
            return false;
        }
    }
}

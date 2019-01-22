package htsjdk.samtools.util.blockcompression;

import htsjdk.samtools.util.BlockCompressedFilePointerUtil;

import java.util.function.BiConsumer;

public class DataBlob {

    private CompressedBlock startBlock;
    private CompressedBlock endBlock;
    private int startByte;
    private int endByte;
    private BiConsumer<Long, Long> callback = null;

    public DataBlob(CompressedBlock startBlock) {
        this.startBlock = startBlock;
        this.startByte = startBlock.getUncompressedSize();
    }

    public DataBlob(CompressedBlock startBlock, BiConsumer<Long,Long> callback) {
        this(startBlock);
        this.callback  = callback;
    }


    public void setEndBlock(CompressedBlock endBlock) {
        this.endBlock = endBlock;
        this.endByte = endBlock.getUncompressedSize();
    }


    private long getStartPointer(){
        return BlockCompressedFilePointerUtil.makeFilePointer(startBlock.getBlockStart(), startByte);
    }

    private long getEndPointer(){
        return BlockCompressedFilePointerUtil.makeFilePointer(endBlock.getBlockStart(), endByte);
    }

    public boolean runCallback(){
        if (endBlock ==null) return false;

        if (CompressedBlock.BlockStatus.WRITTEN.equals(startBlock.getStatus()) && CompressedBlock.BlockStatus.WRITTEN.equals(endBlock.getStatus())){

            if (this.callback != null){
                this.callback.accept(getStartPointer(), getEndPointer());
            }

            return true;
        }else{
            return  false;
        }
    }



}

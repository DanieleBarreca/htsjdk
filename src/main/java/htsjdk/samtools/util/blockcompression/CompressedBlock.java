package htsjdk.samtools.util.blockcompression;

import htsjdk.samtools.util.BlockCompressedStreamConstants;

public class CompressedBlock {

    public enum BlockStatus {
        NEW,
        PARTIAL,
        FULL,
        SUBMITTED,
        DEFLATING,
        DEFLATED,
        WRITTEN,
        ERROR
    }

    private long blockStart;
    protected final int id;

    private BlockStatus status = BlockStatus.NEW;

    private final byte[] uncompressedBuffer = new byte[BlockCompressedStreamConstants.DEFAULT_UNCOMPRESSED_BLOCK_SIZE];
    private int uncompressedSize = 0;

    private final byte[] compressedBuffer =new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE -
                    BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH];
    private int compressedSize = 0;

    private long crc =0;

    private final BlockCompressedParallelOutputStream owner;


    public CompressedBlock(BlockCompressedParallelOutputStream owner, int id) {
        this.owner=owner;
        this.id=id;
    }

    public int getUncompressedSize() {
        return uncompressedSize;
    }

    public byte[] getUncompressedBuffer() {
        return uncompressedBuffer;
    }

    //Return number of bytes accepted
    public int addUncompressedBytes(byte[] bytes, int startIndex, int numBytes) {
        this.setStatus(BlockStatus.PARTIAL);

        assert (uncompressedSize < uncompressedBuffer.length);
        final int bytesToWrite = Math.min(uncompressedBuffer.length - uncompressedSize, numBytes);
        System.arraycopy(bytes, startIndex, uncompressedBuffer, uncompressedSize, bytesToWrite);
        uncompressedSize += bytesToWrite;
        if (uncompressedSize == uncompressedBuffer.length) {
            status=BlockStatus.FULL;
        }

        return bytesToWrite;
    }

    public BlockStatus getStatus() {
        return status;
    }

    public void setStatus(BlockStatus status) {
        this.status = status;
    }

    public long getBlockStart() {
        return blockStart;
    }

    public void setBlockStart(long blockStart) {
        this.blockStart = blockStart;
    }

    public int getCompressedSize() {
        return compressedSize;
    }

    public void setCompressedSize(int compressedSize) {
        this.compressedSize = compressedSize;
    }

    public byte[] getCompressedBuffer() {
        return compressedBuffer;
    }

    public BlockCompressedParallelOutputStream getOwner() {
        return owner;
    }

    public long getCrc() {
        return crc;
    }

    public void setCrc(long crc) {
        this.crc = crc;
    }

    public void complete(BlockStatus status){
        this.setStatus(status);
        this.owner.notifyDeflateJobComplete(this);
    }
}

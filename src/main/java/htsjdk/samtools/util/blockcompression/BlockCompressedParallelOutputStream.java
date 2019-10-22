package htsjdk.samtools.util.blockcompression;



import htsjdk.samtools.util.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class BlockCompressedParallelOutputStream extends BlockCompressedOutputStream{

    private static final Log log = Log.getInstance(BlockCompressedParallelOutputStream.class);

    private AtomicBoolean flushing = new AtomicBoolean(false);
    private AtomicBoolean closing = new AtomicBoolean(false);
    private AtomicBoolean closed = new AtomicBoolean(false);
    private AtomicBoolean error = new AtomicBoolean(false);

    private Path file = null;

    // Really a local variable, but allocate once to reduce GC burden.
    private final byte[] singleByteArray = new byte[1];

    CompressedBlock currentBlock = new CompressedBlock(this, 0);
    int lastWrittenBlock = -1;
    int lastSubmittedBlock = -1;

    private Set<CompressedBlock> blocksPendingWriting = new TreeSet<>((CompressedBlock o1, CompressedBlock o2) -> (o1.id - o2.id));

    private LinkedList<DataBlob> blobs = new LinkedList<>();

    private long mBlockAddress = 0;

    private BinaryCodec codec;

    private BlockDeflaterCallable.TaskQueue deflateQueue;

    /**
     * Prepare to compress at the given compression level
     */
    protected BlockCompressedParallelOutputStream(final Path path, BlockDeflaterCallable.TaskQueue queue) {

        this.file = path;
        codec = new BinaryCodec(path, true);
        this.deflateQueue = queue;
    }

    /**
     * Creates the output stream.
     * @param os output stream to create a BlockCompressedOutputStream from
     * @param file file to which to write the output or null if not available
     */
    public BlockCompressedParallelOutputStream(final OutputStream os, final Path file,BlockDeflaterCallable.TaskQueue queue) {

        this.file = file;
        codec = new BinaryCodec(os);
        if (file != null) {
            codec.setOutputFileName(file.toAbsolutePath().toUri().toString());
        }
        this.deflateQueue = queue;
    }


    @Override
    public void write(final int bite) throws IOException {
        singleByteArray[0] = (byte)bite;
        write(singleByteArray);
    }


    @Override
    public void write(final byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public synchronized void write(final byte[] bytes, int startIndex, int numBytes) throws IOException {
        check();

        while (numBytes>0) {
            int writtenBytes = currentBlock.addUncompressedBytes(bytes, startIndex, numBytes);

            if (CompressedBlock.BlockStatus.FULL.equals(currentBlock.getStatus())) {
                try {

                    submitDeflateJob(currentBlock);
                    currentBlock = new CompressedBlock(this, currentBlock.id+1);
                } catch (InterruptedException e) {
                    log.error(constructErrorMessage(e), e);
                    closeExceptionally(e);
                }
            }

            startIndex += writtenBytes;
            numBytes -= writtenBytes;
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        check();
        flushInternal(false);
    }

    /**
     * close() must be called in order to flush any remaining buffered bytes.  An unclosed file will likely be
     * defective.
     *
     */
    @Override
    public synchronized void close() throws IOException {
        check();
        flushInternal(true);
    }

    @Override
    public synchronized long getPosition() {
        blockWhileWriting();

        return BlockCompressedFilePointerUtil.makeFilePointer(mBlockAddress, currentBlock.getUncompressedSize());
    }

    private synchronized void blockWhileWriting(){
        while (!this.error.get() && !this.deflateQueue.isError() && this.lastSubmittedBlock!=this.lastWrittenBlock){}
    }

    private void check() throws IOException {
        if (this.error.get()) throw new IOException("Stream has been corrupted");
        if (this.deflateQueue.isError()) throw new IOException("The workers have been interrupted");
        if (this.flushing.get()) throw new IOException("The stream is flushing data");
        if (this.closed.get()  || this.closing.get()) throw new IOException("The stream has been closed");
    }

    private void closeExceptionally(Throwable e) throws IOException {
        closeExceptionally();
        throw new IOException(e);
    }

    private void closeExceptionally(){
        try {
            this.codec.close();
        }finally {
            this.error.set(true);
        }
    }

    private synchronized void flushInternal(boolean close) throws IOException {
        if (close) this.closing.set(true);

        if (CompressedBlock.BlockStatus.PARTIAL.equals(currentBlock.getStatus())){
            this.flushing.set(true);
            try {
                submitDeflateJob(currentBlock);
                currentBlock = new CompressedBlock(this, currentBlock.id+1);
            } catch (InterruptedException e) {
                log.error(constructErrorMessage(e), e);
                closeExceptionally(e);
            }
        }else if (close){
            closeInternal();
        }

        while ( (this.flushing.get() || (close && this.closing.get())) &&
                !this.error.get() && !this.deflateQueue.isError() ){}

        if (this.error.get()) throw new IOException("Stream has been corrupted");
        if (this.deflateQueue.isError()) throw new IOException("The workers have been interrupted");

    }


    private synchronized void closeInternal() throws IOException{
        
        if (!this.closed.get()) {
            try {
                codec.writeBytes(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
                codec.close();

                // Can't re-open something that is not a regular file, e.g. a named pipe or an output stream
                if (this.file == null || !Files.isRegularFile(this.file)) return;

                if (BlockCompressedInputStream.checkTermination(this.file) !=
                        BlockCompressedInputStream.FileTermination.HAS_TERMINATOR_BLOCK) {
                    throw new IOException("Terminator block not found after closing BGZF file " + this.file);
                }

            } catch (Throwable e) {
                log.error(constructErrorMessage(e), e);
                closeExceptionally(e);
            } finally {
                this.closed.set(true);
                this.closing.set(false);
            }
        }
    }


    public synchronized void startBlob(BiConsumer<Long, Long> callback){
        synchronized (blobs) {
            blobs.add(new DataBlob(currentBlock, callback));
        }
    }

    public synchronized void endBlob(){
        synchronized (blobs) {
            blobs.peekLast().setEndBlock(currentBlock);
        }
    }


    protected void notifyDeflateJobComplete(CompressedBlock completedBlock) {
        if (this.error.get()) return;

        if (CompressedBlock.BlockStatus.ERROR.equals(completedBlock.getStatus())) {
            log.error(constructErrorMessage("Write error"), "Error while deflating block ",completedBlock.id);
            closeExceptionally();
        }else if (CompressedBlock.BlockStatus.DEFLATED.equals(completedBlock.getStatus())) {
            synchronized (blocksPendingWriting) {
                blocksPendingWriting.add(completedBlock);

                Iterator<CompressedBlock> blockIterator = blocksPendingWriting.iterator();
                while (blockIterator.hasNext()) {
                    CompressedBlock block = blockIterator.next();
                    if (block.id == lastWrittenBlock + 1) {
                        try {
                            block.setBlockStart(mBlockAddress);
                            mBlockAddress += writeGzipBlock(block);
                            block.setStatus(CompressedBlock.BlockStatus.WRITTEN);

                            lastWrittenBlock = block.id;
                            this.notifyBlockWritten();
                            blockIterator.remove();
                        } catch (Throwable e) {
                            log.error(e,constructErrorMessage("Write error"), "Error while writing block to disk ",block.id);
                            block.setStatus(CompressedBlock.BlockStatus.ERROR);
                            closeExceptionally();
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    private void notifyBlockWritten(){
        synchronized (blobs) {
            Iterator<DataBlob> blobIterator = blobs.iterator();

            while (blobIterator.hasNext()) {
                DataBlob dataBlob = blobIterator.next();
                if (!dataBlob.runCallback()) {
                    break;
                }
                blobIterator.remove();
            }
        }

        if (!this.error.get() && lastWrittenBlock==lastSubmittedBlock) {
            if (this.flushing.get()) {
                try {
                    codec.getOutputStream().flush();
                } catch (IOException e) {
                    log.error(constructErrorMessage(e), e);
                    closeExceptionally();
                }
                this.flushing.set(false);
            }
            if (this.closing.get()) {
                try {
                    this.closeInternal();
                } catch (IOException e) {
                    log.error(constructErrorMessage(e), e);
                    closeExceptionally();
                }
            }
        }
    }

    private void submitDeflateJob(CompressedBlock block) throws InterruptedException{
        block.setStatus(CompressedBlock.BlockStatus.SUBMITTED);
        lastSubmittedBlock=block.id;
        this.deflateQueue.put(block);
    }


    private int writeGzipBlock(CompressedBlock block) {
        // Init gzip header
        codec.writeByte(BlockCompressedStreamConstants.GZIP_ID1);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_ID2);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_CM_DEFLATE);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_FLG);
        codec.writeInt(0); // Modification time
        codec.writeByte(BlockCompressedStreamConstants.GZIP_XFL);
        codec.writeByte(BlockCompressedStreamConstants.GZIP_OS_UNKNOWN);
        codec.writeShort(BlockCompressedStreamConstants.GZIP_XLEN);
        codec.writeByte(BlockCompressedStreamConstants.BGZF_ID1);
        codec.writeByte(BlockCompressedStreamConstants.BGZF_ID2);
        codec.writeShort(BlockCompressedStreamConstants.BGZF_LEN);
        final int totalBlockSize = block.getCompressedSize() + BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH +
                BlockCompressedStreamConstants.BLOCK_FOOTER_LENGTH;

        // I don't know why we store block size - 1, but that is what the spec says
        codec.writeShort((short)(totalBlockSize - 1));
        codec.writeBytes(block.getCompressedBuffer(), 0, block.getCompressedSize());
        codec.writeInt((int)block.getCrc());
        codec.writeInt(block.getUncompressedSize());
        return totalBlockSize;
    }

    private String constructErrorMessage(final String msg) {
        final StringBuilder sb;
        if (msg != null) {
            sb = new StringBuilder(msg).append("; ");
        }else{
            sb = new StringBuilder();
        }
        sb.append("BlockCompressedParallelOutputStream ");
        if (file != null) {
            sb.append("file: ").append(file.getFileName());
        } else  {
            sb.append("streamed file (filename not available)");
        }
        return sb.toString();
    }

    private String constructErrorMessage(final Throwable e) {
        return constructErrorMessage(e.getMessage());
    }
}

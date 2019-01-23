package htsjdk.samtools.util.blockcompression;

import htsjdk.samtools.util.Log;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

public class BlockDeflaterCallable implements Runnable {
    private static final Log log = Log.getInstance(BlockDeflaterCallable.class);


    private final Deflater deflater;

    // A second deflater is created for the very unlikely case where the regular deflation actually makes
    // things bigger, and the compressed block is too big.  It should be possible to downshift the
    // primary deflater to NO_COMPRESSION level, recompress, and then restore it to its original setting,
    // but in practice that doesn't work.
    // The motivation for deflating at NO_COMPRESSION level is that it will predictably produce compressed
    // output that is 10 bytes larger than the input, and the threshold at which a block is generated is such that
    // the size of tbe final gzip block will always be <= 64K.  This is preferred over the previous method,
    // which would attempt to compress up to 64K bytes, and if the resulting compressed block was too large,
    // try compressing fewer input bytes (aka "downshifting').  The problem with downshifting is that
    // getFilePointer might return an inaccurate value.
    // I assume (AW 29-Oct-2013) that there is no value in using hardware-assisted deflater for no-compression mode,
    // so just use JDK standard.
    private final Deflater noCompressionDeflater = new Deflater(Deflater.NO_COMPRESSION, true);
    private final CRC32 crc32 = new CRC32();

    private  TaskQueue input;


    public BlockDeflaterCallable(Deflater deflater, TaskQueue input) {
        this.deflater = deflater;
        this.input = input;
    }

    public void run() {
        while (!input.isError() && (!input.isDone() || input.size() != 0)) {

            try {
                CompressedBlock block = input.take();
                try {
                    block.setStatus(CompressedBlock.BlockStatus.DEFLATING);

                    crc32.reset();
                    crc32.update(block.getUncompressedBuffer(),0,block.getUncompressedSize());
                    block.setCrc(crc32.getValue());

                    // Compress the input
                    deflater.reset();
                    deflater.setInput(block.getUncompressedBuffer(), 0, block.getUncompressedSize());
                    deflater.finish();
                    int compressedSize = deflater.deflate(block.getCompressedBuffer(), 0, block.getCompressedBuffer().length);

                    // If it didn't all fit in compressedBuffer.length, set compression level to NO_COMPRESSION
                    // and try again.  This should always fit.
                    if (!deflater.finished()) {
                        noCompressionDeflater.reset();
                        noCompressionDeflater.setInput(block.getUncompressedBuffer(), 0, block.getUncompressedSize());
                        noCompressionDeflater.finish();
                        compressedSize = noCompressionDeflater.deflate(block.getCompressedBuffer(), 0, block.getCompressedBuffer().length);
                        if (!noCompressionDeflater.finished()) {
                            throw new IllegalStateException("unpossible");
                        }
                    }

                    block.setCompressedSize(compressedSize);
                    block.complete(CompressedBlock.BlockStatus.DEFLATED);
                } catch (Throwable e) {
                    block.complete(CompressedBlock.BlockStatus.ERROR);
                    log.error(e,"Error while processing block ",block.id);
                }
            } catch (InterruptedException e) {
                input.setError();
                log.error(e,"Task was interrupted");

            }
        }

    }

    public static class TaskQueue extends ArrayBlockingQueue<CompressedBlock> {

        private AtomicBoolean isDone = new AtomicBoolean(false);
        private AtomicBoolean isError= new AtomicBoolean(false);

        public TaskQueue(int capacity) {
            super(capacity);
        }

        @Override
        public boolean add(CompressedBlock compressedBlock) {
            if (isDone.get()) throw new IllegalStateException();

            return super.add(compressedBlock);
        }

        @Override
        public boolean offer(CompressedBlock compressedBlock) {
            if (isDone.get()) return false;

            return super.offer(compressedBlock);
        }

        @Override
        public void put(CompressedBlock compressedBlock) throws InterruptedException {
            if (isDone.get()) throw new InterruptedException();

            super.put(compressedBlock);
        }

        @Override
        public boolean offer(CompressedBlock compressedBlock, long timeout, TimeUnit unit) throws InterruptedException {
            if (isDone.get()) return false;

            return super.offer(compressedBlock, timeout, unit);
        }

        public boolean isDone() {
            return isDone.get();
        }

        public void setDone() {
            isDone.set(true);
        }

        public boolean isError() {
            return isError.get();
        }

        public void setError() {
            this.isError.set(true);
        }
    }

}

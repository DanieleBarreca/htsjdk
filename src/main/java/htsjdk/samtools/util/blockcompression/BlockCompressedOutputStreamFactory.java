package htsjdk.samtools.util.blockcompression;

import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.zip.DeflaterFactory;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.Deflater;

public class BlockCompressedOutputStreamFactory {

    private static int defaultCompressionLevel = BlockCompressedStreamConstants.DEFAULT_COMPRESSION_LEVEL;
    private static DeflaterFactory defaultDeflaterFactory = new DeflaterFactory();

    private static BlockDeflaterCallable.TaskQueue deflateQueue = null;
    private static ExecutorService executorService = null;
    private static boolean multiThread = false;

    static {
        initMultiThreading(0);
    }

    /**
     * Sets the GZip compression level for subsequent BlockCompressedOutputStream object creation
     * that do not specify the compression level.
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public static void setDefaultCompressionLevel(final int compressionLevel) {
        if (compressionLevel < Deflater.NO_COMPRESSION || compressionLevel > Deflater.BEST_COMPRESSION) {
            throw new IllegalArgumentException("Invalid compression level: " + compressionLevel);
        }
        defaultCompressionLevel = compressionLevel;
    }

    public static int getDefaultCompressionLevel() {
        return defaultCompressionLevel;
    }

    /**
     * Sets the default {@link DeflaterFactory} that will be used for all instances unless specified otherwise in the constructor.
     * If this method is not called the default is a factory that will create the JDK {@link Deflater}.
     * @param deflaterFactory non-null default factory.
     */
    public static void setDefaultDeflaterFactory(final DeflaterFactory deflaterFactory) {
        if (deflaterFactory == null) {
            throw new IllegalArgumentException("null deflaterFactory");
        }
        defaultDeflaterFactory = deflaterFactory;
    }

    public static DeflaterFactory getDefaultDeflaterFactory() {
        return defaultDeflaterFactory;
    }

    public static void initMultiThreading(int numberOfThreads){
        if (BlockCompressedOutputStreamFactory.executorService ==null ){
            if (numberOfThreads<1){
                multiThread=false;
                deflateQueue = null;
            }else {
                multiThread = true;
                deflateQueue = new BlockDeflaterCallable.TaskQueue(numberOfThreads);
                executorService = Executors.newFixedThreadPool(numberOfThreads);

                for (int i = 0; i < numberOfThreads; i++) {
                    executorService.submit(new BlockDeflaterCallable(defaultDeflaterFactory.makeDeflater(defaultCompressionLevel, true), deflateQueue));
                }
            }
        }else{
            close();
            initMultiThreading(numberOfThreads);
        }
    }

    public static void reset(){
        close();
        defaultCompressionLevel = BlockCompressedStreamConstants.DEFAULT_COMPRESSION_LEVEL;
        defaultDeflaterFactory = new DeflaterFactory();
        initMultiThreading(2);
    }

    public static void close(){
        if (executorService!=null) {
            deflateQueue.isDone();
            executorService.shutdown();
            deflateQueue = null;
            executorService = null;
        }
    }

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #makeBlockCompressedOutputStream(File)} to specify a custom factory.
     */
    public static BlockCompressedOutputStream makeBlockCompressedOutputStream(final String filename) {

        return makeBlockCompressedOutputStream(new File(filename));
    }

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     */
    public static BlockCompressedOutputStream makeBlockCompressedOutputStream(final File file) {
     if (multiThread){
            return new BlockCompressedParallelOutputStream(IOUtil.toPath(file), deflateQueue);
        }else {
            return new DefaultBlockCompressedOutputStream(IOUtil.toPath(file), defaultDeflaterFactory.makeDeflater(defaultCompressionLevel, true));
        }
    }


    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     * Note: this constructor uses the default {@link DeflaterFactory}, see {@link #getDefaultDeflaterFactory()}.
     * Use {@link #makeBlockCompressedOutputStream(OutputStream, File)} to specify a custom factory.
     *
     * @param file may be null
     */
    public static BlockCompressedOutputStream makeBlockCompressedOutputStream(final OutputStream os, final File file) {
        return makeBlockCompressedOutputStream(os, IOUtil.toPath(file));
    }

    /**
     * Creates the output stream.
     * @param os output stream to create a BlockCompressedOutputStream from
     * @param file file to which to write the output or null if not available
     */
    public static BlockCompressedOutputStream makeBlockCompressedOutputStream(final OutputStream os, final Path file) {
        if (multiThread){
            return new BlockCompressedParallelOutputStream(os, file,deflateQueue);
        }else {
            return new DefaultBlockCompressedOutputStream(os, file, defaultDeflaterFactory.makeDeflater(defaultCompressionLevel,true));
        }

    }

    /**
     *
     * @param location May be null.  Used for error messages, and for checking file termination.
     * @param output May or not already be a BlockCompressedOutputStream.
     * @return A BlockCompressedOutputStream, either by wrapping the given OutputStream, or by casting if it already
     *         is a BCOS.
     */
    public static BlockCompressedOutputStream maybeBgzfWrapOutputStream(final File location, OutputStream output) {
        if (!(output instanceof BlockCompressedOutputStream)) {
            return makeBlockCompressedOutputStream(output, location);
        } else {
            return (BlockCompressedOutputStream) output;
        }
    }


}

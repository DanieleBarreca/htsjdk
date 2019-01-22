package htsjdk.samtools.util.blockcompression;

import htsjdk.samtools.util.LocationAware;

import java.io.IOException;
import java.io.OutputStream;

public abstract class BlockCompressedOutputStream extends OutputStream implements LocationAware {

    @Override
    public abstract void write(byte[] b) throws IOException;

    @Override
    public abstract void write(byte[] b, int off, int len) throws IOException;

    @Override
    public abstract void flush() throws IOException;

    @Override
    public abstract void close() throws IOException;

    @Override
    public abstract void write(int b) throws IOException;
}

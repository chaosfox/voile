package org.voile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * wraps a nio.ByteBuffer in a InputStream
 *
 * @author fox
 */
public class ByteBufferInputStream extends InputStream
{
    private final ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer buf){
        this.buf = buf;
    }

    @Override
    public int read() throws IOException {
        return buf.hasRemaining() ? buf.get() : -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        // Read only what's left
        len = Math.min(len, buf.remaining());
        if(len == 0)return -1;
        buf.get(bytes, off, len);
        return len;
    }
}

package org.voile;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.voile.MemoryPool.Block;

/**
 * @author fox
 */
public class VoileFile<K extends Serializable, V extends Serializable> {

    private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int INDEX_ENTRY_SIZE = 3 * INT_SIZE;

    private static final int INDEX_START = 2 * INT_SIZE;

    private static final Comparator<Entry> dataPointerComparator = new Comparator<Entry>() {
        @Override
        public int compare(Entry e1, Entry e2) {
            return e1.data.start - e2.data.start;
        }
    };

    private final RandomAccessFile file;
    private final FileChannel chan;

    private final HashMap<K, Entry> index;
    private final MemoryPool headerSpace;
    private final MemoryPool dataSpace;

    public VoileFile(File f) throws IOException {
        boolean newFile = !f.exists();
        file = new RandomAccessFile(f, "rw");
        chan = file.getChannel();
        index = new HashMap<K, Entry>();

        if (newFile) {
            int dataStartPointer = 5 * INDEX_ENTRY_SIZE;
            headerSpace = new MemoryPool(INDEX_START, dataStartPointer, false);
            dataSpace = new MemoryPool(dataStartPointer, 2 * dataStartPointer, true);
            updateMainHeader();
        }
        else {
            // read existing file
            final int numEntries = file.readInt();
            final int dataStartPointer = file.readInt();

            ArrayList<Entry> entry_list = new ArrayList<Entry>(numEntries);

            // read index, numEntries x (dataPointer, dataLength, keySize) 
            for (int i = 0; i < numEntries; i++) {
                Block header = new Block((int) file.getFilePointer(), INDEX_ENTRY_SIZE);
                Block data = new Block(file.readInt(), file.readInt());
                Entry e = new Entry(header, data, file.readInt());

                @SuppressWarnings({"unchecked"})
                K key = (K) bin2object(readKey(e));

                index.put(key, e);
                entry_list.add(e);
            }
            int p = (int) file.getFilePointer();
            headerSpace = new MemoryPool(p, dataStartPointer, false);

            int end = (int) file.length();
            dataSpace = new MemoryPool(end, end, true);

            // re-construct the free space pool based on the
            // holes missing between the index entries
            Collections.sort(entry_list, dataPointerComparator);
            int d_pos = dataStartPointer;
            for (Entry e : entry_list) {
                dataSpace.free(new Block(d_pos, e.data.start - d_pos));
                d_pos = e.data.start + e.data.length;
            }
            dataSpace.free(new Block(d_pos, end - d_pos));
        }
    }

    public int numEntries() {
        return index.size();
    }

    public Set<K> keySet() {
        return index.keySet();
    }

    public boolean containsKey(K key) {
        return index.containsKey(key);
    }

    public V put(K key, V value) throws IOException {

        Entry e = index.get(key);
        ByteBuffer key_data = object2bin(key);
        ByteBuffer value_data = object2bin(value);

        //Logger.getLogger(this.getClass().getName()).log(Level.INFO, "put key[{0}] [{1}]", new Object[]{key, value});

        V old_value = null;

        // if there's already a entry at this key
        if (e != null) {

            // if we got enough space, update
            if (value_data.remaining() + e.keySize <= e.data.length) {

                //noinspection unchecked
                old_value = (V) bin2object(readValue(e)); // get old value first

                // split the data block
                Block[] split_block = Block.splitBlock(e.data, value_data.remaining() + e.keySize);

                dataSpace.free(split_block[1]); // free extra space
                e.data = split_block[0]; // store new block

                // write new data
                chan.write(value_data, e.data.start + e.keySize);
                writeEntry(e);

                return old_value;
            }
            // else, we need to remove and then insert again
            old_value = remove(key);
        }
        // insert new

        e = allocate(key_data.remaining(), value_data.remaining());

        writeData(e, key_data, value_data);
        writeEntry(e);

        index.put(key, e);

        updateMainHeader();

        return old_value;
    }

    public V get(K key) throws IOException {
        Entry e = index.get(key);
        if (e == null) return null;
        //noinspection unchecked
        return (V) bin2object(readValue(e));
    }

    public void close() throws IOException {
        chan.close();
        file.close();
    }

    public V remove(K key) throws IOException {

        final Entry e = index.get(key);
        if (e == null) return null;

        //Logger.getLogger(this.getClass().getName()).log(Level.INFO, "remove [{0}]", key);

        @SuppressWarnings({"unchecked"})
        V old_value = (V) bin2object(readValue(e));

        dataSpace.free(e.data);

        removeEntry(e);

        index.remove(key);
        updateMainHeader();
        return old_value;
    }

    /**
     * removes a entry from the header
     * it swaps the space with the last entry
     * to keep the header without holes
     *
     * @param e entry
     * @throws java.io.IOException when unlucky
     */
    private void removeEntry(Entry e) throws IOException {

        Entry max = e;
        for (Entry i : index.values()) {
            if (i.header.start > max.header.start)
                max = i;
        }
        Block last_p = e.header;
        if (max != e) {
            last_p = max.header;
            max.header = e.header;
            writeEntry(max);
        }
        headerSpace.free(last_p);
    }


    private void updateMainHeader() throws IOException {
        file.seek(0);
        file.writeInt(index.size());
        file.writeInt(headerSpace.getLimit());
    }

    private Entry allocate(int keySize, int valueSize) throws IOException {

        int size = keySize + valueSize;

        freeHeaderSpace();
        Block header = headerSpace.allocate(INDEX_ENTRY_SIZE);
        Block data = dataSpace.allocate(size);
        return new Entry(header, data, keySize);
    }

    private void freeHeaderSpace() throws IOException {
        while (!headerSpace.checkSpace(INDEX_ENTRY_SIZE)) {
            Entry f = findBlockAt(headerSpace.getLimit());

            if (f == null) { // freed maybe ?
                Block b = dataSpace.allocateAt(headerSpace.getLimit());

                if (b != null) {
                    headerSpace.free(b); // pass the space to the header
                    continue;
                } else {
                    throw new IOException("Corrupted: Couldn't get extra space for the header.");
                }
            }

            // find a new place to the data
            Block data = dataSpace.allocate(f.data.length);

            // read the data
            ByteBuffer key_data = readKey(f);
            ByteBuffer value_data = readValue(f);

            // tell the header the space is free
            headerSpace.free(f.data);

            f.data = data; // set new block

            // and transfer the data
            writeData(f, key_data, value_data);
            writeEntry(f);
        }
    }

    private Entry findBlockAt(int targetFp) {
        for (Entry e : index.values()) {
            if (e.data.start == targetFp) return e;
        }
        return null;
    }

    private void writeEntry(Entry e) throws IOException {
        file.seek(e.header.start);
        file.writeInt(e.data.start);
        file.writeInt(e.data.length);
        file.writeInt(e.keySize);
    }

    private void writeData(Entry e, ByteBuffer key, ByteBuffer value) throws IOException {
        chan.write(key, e.data.start);
        chan.write(value, e.data.start + e.keySize);
    }

    private ByteBuffer readKey(Entry e) throws IOException {
        ByteBuffer key_data = ByteBuffer.allocate(e.keySize);
        chan.read(key_data, e.data.start);
        key_data.rewind();
        return key_data;
    }

    private ByteBuffer readValue(Entry e) throws IOException {
        ByteBuffer value_data = ByteBuffer.allocate(e.data.length - e.keySize);
        chan.read(value_data, e.data.start + e.keySize);
        value_data.rewind();
        return value_data;
    }

    private ByteBuffer object2bin(Serializable o) throws IOException {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(bao).writeObject(o);
        } catch (IOException ex) {
            throw ex;
        }
        return ByteBuffer.wrap(bao.toByteArray());
    }

    private Serializable bin2object(ByteBuffer blob) throws IOException {
        if (blob == null) return null;

        try {
            return (Serializable) new ObjectInputStream(new ByteBufferInputStream(blob)).readObject();
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }
    }

    static class Entry {
        final int keySize;

        Block header;
        Block data;

        Entry(Block header, Block data, int ks) {
            this.header = header;
            this.data = data;
            keySize = ks;
        }
    }
}

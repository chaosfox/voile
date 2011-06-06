package org.voile;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * @author fox
 */
public class VoileFile<K extends Serializable, V extends Serializable> {

    private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int INDEX_ENTRY_SIZE = INT_SIZE + INT_SIZE + INT_SIZE;

    private static final int INDEX_START = INT_SIZE + INT_SIZE;

    private static final Comparator<Entry> dataPointerComparator = new Comparator<Entry>() {
        @Override
        public int compare(Entry e1, Entry e2) {
            return e1.dataPointer - e2.dataPointer;
        }
    };

    private final RandomAccessFile file;
    private final FileChannel chan;

    private final HashMap<K, Entry> index;
    private final MemManager headerSpace;
    private final MemManager dataSpace;

    public VoileFile(File f) throws IOException {
        boolean newFile = !f.exists();
        file = new RandomAccessFile(f, "rw");
        chan = file.getChannel();
        index = new HashMap<K, Entry>();

        if (newFile) {
            int dataStartPointer = 5 * INDEX_ENTRY_SIZE;
            headerSpace = new MemManager(INDEX_START, dataStartPointer, false);
            dataSpace = new MemManager(dataStartPointer, 2 * dataStartPointer, true);
            updateMainHeader();
        }
        else {
            final int numEntries = file.readInt();
            final int dataStartPointer = file.readInt();

            ArrayList<Entry> entry_list = new ArrayList<Entry>(numEntries);

            for (int i = 0; i < numEntries; i++) {
                Entry e = new Entry((int) file.getFilePointer(),
                        file.readInt(), file.readInt(), file.readInt());

                @SuppressWarnings({"unchecked"})
                K key = (K) bin2object(readKey(e));

                index.put(key, e);
                entry_list.add(e);
            }
            int p = (int) file.getFilePointer();
            headerSpace = new MemManager(p, dataStartPointer, false);

            int end = (int) file.length();
            dataSpace = new MemManager(end, end, true);

            Collections.sort(entry_list, dataPointerComparator);
            int d_pos = dataStartPointer;
            for (Entry e : entry_list) {
                dataSpace.free(d_pos, e.dataPointer - d_pos);
                d_pos = e.dataPointer + e.dataSize;
            }
            dataSpace.free(d_pos, end - d_pos);
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
        System.err.println("put key[" + key + "] [" + value + "]");
        V old_value = null;

        if (e != null) { // update

            //noinspection unchecked
            old_value = (V) bin2object(readValue(e));

            // we got enough space to update ?
            if (value_data.remaining() + e.keySize <= e.dataSize) {

                int used = e.keySize + value_data.remaining();
                if (used < e.dataSize) { // free unused
                    dataSpace.free(e.dataPointer + used, e.dataSize - used);
                    e.dataSize = used;
                }
                chan.write(value_data, e.dataPointer + e.keySize);
                writeEntry(e);
                return old_value;
            }
            // else, need remove-insert again
            System.err.print("put--");
            remove(key);
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

        System.err.println("remove [" + key + "]");

        @SuppressWarnings({"unchecked"})
        V old_value = (V) bin2object(readValue(e));

        dataSpace.free(e.dataPointer, e.dataSize);

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
            if (i.headerPointer > max.headerPointer)
                max = i;
        }
        int last_p = e.headerPointer;
        if (max != e) {
            last_p = max.headerPointer;
            max.headerPointer = e.headerPointer;
            writeEntry(max);
        }
        headerSpace.free(last_p, INDEX_ENTRY_SIZE);
    }

    private void updateMainHeader() throws IOException {
        file.seek(0);
        file.writeInt(index.size());
        file.writeInt(headerSpace.limit);
    }

    private Entry allocate(int keySize, int valueSize) throws IOException {

        int size = keySize + valueSize;

        freeHeaderSpace();
        int hp = headerSpace.allocate(INDEX_ENTRY_SIZE);
        int p = dataSpace.allocate(size);
        return new Entry(hp, p, keySize, size);
    }

    private void freeHeaderSpace() throws IOException {
        while (!headerSpace.checkSpace(INDEX_ENTRY_SIZE)) {
            Entry f = findBlockAt(headerSpace.limit);

            if (f == null) { // freed maybe ?
                System.err.println(dataSpace);
                System.err.println(headerSpace);
                int size = dataSpace.allocateAt(headerSpace.limit);

                if (size >= 0) {
                    headerSpace.free(headerSpace.limit, size);
                    System.err.println("found on data " + size);
                    continue;
                } else {
                    throw new IOException("LOL " + size);
                }
            }

            // find a new place to the data
            int dp = dataSpace.allocate(f.dataSize);

            // read the data
            ByteBuffer key_data = readKey(f);
            ByteBuffer value_data = readValue(f);

            // tell the header the space is free
            headerSpace.free(f.dataPointer, f.dataSize);

            f.dataPointer = dp; // set new pointer

            // and transfer the data
            writeData(f, key_data, value_data);
            writeEntry(f);
        }
    }

    private Entry findBlockAt(int targetFp) {
        System.err.println("target " + targetFp);
        for (Entry e : index.values()) {
            System.err.println(e.dataPointer + " " + e.dataSize);
            if (e.dataPointer == targetFp) return e;
        }
        return null;
    }

    private void writeEntry(Entry e) throws IOException {
        file.seek(e.headerPointer);
        file.writeInt(e.dataPointer);
        file.writeInt(e.keySize);
        file.writeInt(e.dataSize);
    }

    private void writeData(Entry e, ByteBuffer key, ByteBuffer value) throws IOException {
        chan.write(key, e.dataPointer);
        chan.write(value, e.dataPointer + e.keySize);
    }

    private ByteBuffer readKey(Entry e) throws IOException {
        ByteBuffer key_data = ByteBuffer.allocate(e.keySize);
        chan.read(key_data, e.dataPointer);
        key_data.rewind();
        return key_data;
    }

    private ByteBuffer readValue(Entry e) throws IOException {
        ByteBuffer value_data = ByteBuffer.allocate(e.dataSize - e.keySize);
        chan.read(value_data, e.dataPointer + e.keySize);
        value_data.rewind();
        return value_data;
    }

    private ByteBuffer object2bin(Serializable o) {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(bao).writeObject(o);
        } catch (IOException ignored) {
            return null;
        }
        return ByteBuffer.wrap(bao.toByteArray());
    }

    private Serializable bin2object(ByteBuffer blob) throws IOException {
        if (blob == null) return null;

        try {
            return (Serializable) new ObjectInputStream(new ByteBufferInputStream(blob)).readObject();
        } catch (ClassNotFoundException ignored) {
            ignored.printStackTrace(); //TODO: is this a problem ?
        }
        return null;
    }

    static class Entry {
        int dataPointer;
        final int keySize;
        int dataSize;
        int headerPointer;

        Entry(int hp, int dp, int ks, int ds) {
            headerPointer = hp;
            dataPointer = dp;
            keySize = ks;
            dataSize = ds;
        }
    }
}

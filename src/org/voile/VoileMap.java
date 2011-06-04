package org.voile;

import java.io.*;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * @author fox
 */
public class VoileMap <K extends Serializable, V extends Serializable> implements Map<K, V> {

    private VoileFile<K,V> vf;
    private final File file;

    public VoileMap(File file) {
        this.file = file;
        try {
            vf = new VoileFile<K,V>(file);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public int size() {
        return vf.numEntries();
    }

    @Override
    public boolean isEmpty() {
        return vf.numEntries() == 0;
    }

    @Override
    public boolean containsKey(Object o) {
        return vf.containsKey((K) o);
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException("Not supported. ever.");
    }

    @Override
    public V get(Object o) {
        try {
            return vf.get((K)o);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public V put(K k, V v) {
        try {
            return vf.put(k,v);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public V remove(Object o) {
        try {
            return vf.remove((K) o);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for(Entry<? extends K, ? extends V> e : map.entrySet())
            put(e.getKey(),e.getValue());
    }

    @Override
    public void clear() {
        try {
            vf.close();
            file.delete();
            file.createNewFile();
            vf = new VoileFile<K,V>(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<K> keySet() {
        return vf.keySet();
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("Not supported. ever.");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("Not supported. ever.");
    }

    public void close() throws IOException {
        vf.close();
    }
}

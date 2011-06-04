package org.voile;


import java.util.TreeSet;

public class MemManager {

    private final TreeSet<Block> freeSpace;

    int limit;
    private final boolean growable; // grow when allocate

    public MemManager(int used, int length, boolean growable) {
        this.limit = length;
        this.growable = growable;

        freeSpace = new TreeSet<Block>();
        free(0, limit);
        allocate(used);
    }

    public int allocate(int size) {
        if (size <= 0) return -1;

        Block b = findFreeBlock(size);

        if (b == null) {
            if (!growable) return -1;

            // create more space
            free(limit, 2 * size);
            b = findFreeBlock(size);
        }

        return checkout(b, size);
    }

    public boolean checkSpace(int size) {
        if (growable) return true;

        Block b = findFreeBlock(size);
        return b != null;
    }

    private int checkout(Block b, int size) {
        int extra = b.length - size;
        int p = b.start;
        freeSpace.remove(b);
        if (extra > 0) {
            b.start = b.start + size;
            b.length = extra;
            freeSpace.add(b);
        }
        return p;
    }

    private Block findFreeBlock(int size) {

        // iterate in order min~max
        for (Block b : freeSpace) {
            if (b.length >= size) {
                return b;
            }
        }
        return null;
    }

    public int findBlockAt(long p) {
        for (Block b : freeSpace) {
            if (b.start == p) {
                checkout(b, b.length);
                return b.length;
            }
        }
        return -1;
    }

    public void free(int p, int size) {

        if (size <= 0) return;

        if (p + size > limit) limit = p + size; // grows with free

        Block nb = new Block(p, size);

        Block prev = freeSpace.floor(nb);
        Block next = freeSpace.higher(nb);

        if (prev != null && prev.start + prev.length > nb.start) {
            new RuntimeException("DEBUG PRV " + prev.start + "+" + prev.length + ">" + nb.start).printStackTrace();
            System.exit(1);
        }

        if (next != null && next.start < nb.start + nb.length) {
            new RuntimeException("DEBUG NEX " + next.start + "<" + nb.start + "+" + nb.length).printStackTrace();
            System.exit(1);
        }

        // prev-nb connect
        if (prev != null && prev.start + prev.length == nb.start) {
            prev.length += nb.length;
            freeSpace.remove(prev);
            nb = prev; // now they are merged
        }

        // nb-next connect
        if (next != null && next.start == nb.start + nb.length) {
            nb.length += next.length;
            freeSpace.remove(next);
        }
        freeSpace.add(nb);
    }

    public String toString() {
        String s = "";
        for (Block b : freeSpace) {
            s += "BLOCK [" + b.start + "] ~ " + b.length + "\n";
        }
        return s;
    }

    static class Block implements Comparable<Block> {

        int start;
        int length;

        Block(int start, int length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public int compareTo(Block sb) {
            return start - sb.start;
        }

        public boolean equals(Object o) {
            if (!(o instanceof Block)) return super.equals(o);

            return start == ((Block) o).start && length == ((Block) o).length;
        }
    }
}

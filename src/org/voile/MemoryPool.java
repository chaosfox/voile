package org.voile;


import java.util.TreeSet;
/**
 * manages a pool of space, provides methods
 * to allocate and free blocks of the pool
 * @author fox
 */
public class MemoryPool {

    private final TreeSet<Block> freeSpace;

    private int limit;
    private final boolean growable;

    /** creates a new pool of space
     * @param offset where to start the count
     * @param length the size of the pool
     * @param growable grow as needed when allocate
     */
    public MemoryPool(int offset, int length, boolean growable) {
        this.limit = length;
        this.growable = growable;

        freeSpace = new TreeSet<Block>();
        free(new Block(offset, limit-offset));
    }

    /**
     * allocates a block from the pool
     * @param size the size of the desired block
     * @return a pointer for the block
     */
    public Block allocate(int size) {
        if (size <= 0) return null;

        Block b = findFreeBlock(size);

        if (b == null) {
            if (!growable) return null;

            // create more space
            free(new Block(limit, size));
            b = findFreeBlock(size);
        }

        return checkout(b, size);
    }

    /** 
     * checks if this pool have a block with enough space
     * @param size the desired block size
     * @return whether it have the block
     */
    public boolean checkSpace(int size) {
        if (growable) return true;

        Block b = findFreeBlock(size);
        return b != null;
    }

    /** 
     * removes the block from this pool,
     * possibly split it if it's bigger than desired
     * @param b the block to remove
     * @param size how much space I actually want
     * @return a pointer for the space
     */
    private Block checkout(Block b, int size) {
        freeSpace.remove(b);
        Block nb = b.split(size);
        if(b.length>0)freeSpace.add(b);
        return nb;
    }

    /** 
     * search for the smallest block with size space
     * @param size the space desired
     * @return the block or null if not found
     */
    private Block findFreeBlock(int size) {

        // iterate in order min~max
        for (Block b : freeSpace) {
            if (b.length >= size) 
                return b;
        }
        return null;
    }

    /** allocate a block at the desired place
     * @param p the position desired to start
     * @return the block or null if there isn't any
     */
    public Block allocateAt(long p) {
        for (Block b : freeSpace) {
            if (b.start == p)
                return checkout(b, b.length);
        }
        return null;
    }

    /**
     * @return the limit of the pool, the max address
     */
    public int getLimit() {
        return limit;
    }

    /**
     * free a block back to the chunk
     * @param b the block to be free
     */
    public void free(Block b) {
        if(b.length <= 0) return;

        if (b.start + b.length > limit) limit = b.start + b.length; // grows with free

        Block prev = freeSpace.floor(b);
        Block next = freeSpace.higher(b);

        if (prev != null && prev.start + prev.length > b.start) {
            new RuntimeException("DEBUG PRV " + prev.start + "+" + prev.length + ">" + b.start).printStackTrace();
            System.exit(1);
        }
        if (next != null && next.start < b.start + b.length) {
            new RuntimeException("DEBUG NEX " + next.start + "<" + b.start + "+" + b.length).printStackTrace();
            System.exit(1);
        }

        if(b.merge(prev))freeSpace.remove(prev);
        if(b.merge(next))freeSpace.remove(next);

        freeSpace.add(b);
    }

    @Override
    public String toString() {
        String s = "";
        for (Block b : freeSpace) {
            s += "BLOCK [" + b.start + "] ~ " + b.length + "\n";
        }
        return s;
    }

    public static class Block implements Comparable<Block> {

        int start;
        int length;

        public Block(int start, int length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public int compareTo(Block sb) {
            return start - sb.start;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Block)) return super.equals(o);

            return start == ((Block) o).start && length == ((Block) o).length;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 53 * hash + this.start;
            hash = 53 * hash + this.length;
            return hash;
        }
        public boolean merge(Block b) {
            if(b == null)return false;
            // left merge
            if(b.start + b.length == this.start) {
                this.start = b.start;
                this.length += b.length;
                return true;
            }
            // right merge
            if (b.start == this.start + this.length) {
                this.length += b.length;
                return true;
            }
            return false;
        }
        public Block split(int size) {
            Block b = new Block(this.start, size);
            this.start += size;
            this.length -= size;
            return b;
        }
    }
}

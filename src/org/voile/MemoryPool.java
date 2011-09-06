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
    private final boolean canGrow;

    /** creates a new pool of space
     * @param offset where to start the count
     * @param length the size of the pool
     * @param canGrow grow as needed when allocate
     */
    public MemoryPool(int offset, int length, boolean canGrow) {
        this.limit = length;
        this.canGrow = canGrow;

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
            if (!canGrow) return null;

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
        if (canGrow) return true;

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

        // split b size bytes
        Block [] ab = Block.splitBlock(b, size);
        
        // if there is extra space, add it back
        if(ab[1].length>0)freeSpace.add(ab[1]);
        return ab[0];
    }

    /** 
     * search for the smallest block with size space
     * @param size the space desired
     * @return the block or null if not found
     */
    private Block findFreeBlock(int size) {

        // iteration is ordered min ~ max
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

        // query adjacent blocks
        Block prev = freeSpace.floor(b);
        Block next = freeSpace.higher(b);

        if (prev != null && prev.start + prev.length > b.start) {
            throw new RuntimeException("Corrupted. DEBUG PRV " + prev.start + "+" + prev.length + ">" + b.start);
        }
        if (next != null && next.start < b.start + b.length) {
            throw new RuntimeException("Corrupted. DEBUG NEX " + next.start + "<" + b.start + "+" + b.length);
        }

        // merge them if possible
        
        Block n = Block.mergeBlocks(b, prev);
        if(n != null) { freeSpace.remove(prev); b = n; }

        n = Block.mergeBlocks(b, next);
        if(n != null) { freeSpace.remove(next); b = n; }

        freeSpace.add(b);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Block b : freeSpace) {
            sb.append(b).append("\n");
        }
        return sb.toString();
    }

    public static class Block implements Comparable<Block> {

        final int start;
        final int length;

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

        @Override
        public String toString() {
            return "BLOCK["+start+":"+length+"]";
        }

        /**
         * merge 2 blocks into one IF their positions
         * are adjacent to each other, otherwise NULL
         * @param a block to merge
         * @param b another block to merge
         * @return merged block or NULL if merge is not possible
         */
        public static Block mergeBlocks(Block a, Block b) {
            if(a == null || b == null)return null;

            // left merge
            if(b.start + b.length == a.start) {
                return new Block(b.start, a.length + b.length);
            }

            // right merge
            if (b.start == a.start + a.length) {
                return new Block(a.start, a.length + b.length);
            }
            return null;
        }

        /**
         * split a block into
         *   a block with size bytes from the start of the original
         *   a block with the extra bytes from the original
         * @param b the original block
         * @param size to split the first block
         * @return an array with the 2 new blocks
         */
        public static Block[] splitBlock(Block b, int size) {
            Block nb = new Block(b.start, size);
            b = new Block(b.start + size, b.length - size);
            return new Block[]{nb, b};
        }
    }
}

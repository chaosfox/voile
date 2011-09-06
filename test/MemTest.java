

import org.voile.MemoryPool;

import java.util.*;
import org.voile.MemoryPool.Block;

/**
 *
 * @author fox
 */
public class MemTest {
    
    public static void main(String []args) {

        //HashMap<Integer,Integer> a = new HashMap<Integer,Integer>();
        HashSet<Block> a = new HashSet<Block>();


        MemoryPool st = new MemoryPool(50,100,true);

        Random rand = new Random();

        for(int i=0;i<10000;i++) {
            if(rand.nextBoolean()) {
                int s = 1 + rand.nextInt(600);
                Block p = st.allocate(s);
                System.out.println("got block on "+p+" size "+s);
                boolean old = a.add(p);
                if(!old) {
                    System.out.println("leaking "+p+" size "+old);
                }
            } else {
                Iterator<Block> it = a.iterator();
                for(int j=0;j<rand.nextInt(5);j++) {
                    if(!it.hasNext())break;
                    Block e = it.next();
                    st.free(e);
                    System.out.println("free "+e);
                    it.remove();
                }
            }
        }

        System.out.println("\n"+st);

        for(Block e : a){
            System.out.println("had "+e);
             st.free(e);
        }

        System.out.println("\n"+st);
    }

    
}

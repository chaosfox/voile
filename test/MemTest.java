

import org.voile.MemManager;

import java.util.*;

/**
 *
 * @author fox
 */
public class MemTest {
    
    public static void main(String []args) {

        HashMap<Integer,Integer> a = new HashMap<Integer,Integer>();


        MemManager st = new MemManager(50,100,true);

        Random rand = new Random();

        for(int i=0;i<10000;i++) {
            if(rand.nextBoolean()) {
                int s = 1 + rand.nextInt(600);
                int p = st.allocate(s);
                System.out.println("got block on "+p+" size "+s);
                Integer old = a.put(p,s);
                if(old != null) {
                    System.out.println("leaking "+p+" size "+old);
                }
            } else {
                Iterator<Map.Entry<Integer,Integer>> it = a.entrySet().iterator();
                for(int j=0;j<rand.nextInt(5);j++) {
                    if(!it.hasNext())break;
                    Map.Entry<Integer,Integer> e = it.next();
                    st.free(e.getKey(),e.getValue());
                    System.out.println("free "+e.getKey()+" "+e.getValue());
                    it.remove();
                }
            }
        }

        System.out.println("\n"+st);

        for(Map.Entry<Integer,Integer> e : a.entrySet()){
            System.out.println("had "+e.getKey()+" "+e.getValue());
             st.free(e.getKey(),e.getValue());
        }

        System.out.println("\n"+st);
    }

    
}

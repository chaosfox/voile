package test;

import org.voile.VoileMap;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


/**
 *
 * @author fox
 */
public class Main {

    static final int COUNT = 15500;
    
    public static void main(String[] args) throws Exception {
        
        File a = new File("test.txt");
        a.delete();
        
        VoileMap<String,String> vm = new VoileMap<String,String>(a);
        HashMap<String,String> hm = new HashMap<String,String>();
        
        Random rand = new Random();
        for(int i=0;i<COUNT;i++) {

            String key = mkkey(rand.nextInt() % 50);
            String value = repeat("V",rand.nextInt()%50);
            if(rand.nextBoolean()) { // insert
                vm.put(key, value);
                hm.put(key, value);
            } else { // remove
                vm.remove(key);
                hm.remove(key);
            }
            if(rand.nextInt(50) == 1) { // close & re-open
                vm.close();
                vm = new VoileMap<String,String>(a);
                System.err.println("RESTART");
            }

            checkThem(hm, vm);
        }
    }

    static void checkThem(Map<String,String> a, Map<String,String> b) {

       for(String key : a.keySet()) {

               String s1 = a.get(key);
               String s2 = b.get(key);
               if(!s1.equals(s2)){
                   new Exception("shit").printStackTrace();
                   System.err.println(" k["+key+"] -> ["+s1+"] = ["+s2+"]");
                   System.exit(1);
               } else {
                   //System.out.println("ok");
               }
       }
       if(!a.equals(b)) {
            new Exception("equals shit").printStackTrace();
           System.exit(1);
       }
    }

    static String repeat(String x, int n) {
        String r = "";
        for(int i=0;i<n;i++) {
            r += x;
        }
        return r;
    }
    static String mkkey(int n) {
        String r = "";
        for(int i=0;i<n;i++) {
            r += i;
        }
        return r;
    }
}

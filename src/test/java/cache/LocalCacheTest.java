package cache;

public class LocalCacheTest {
    
    
    public static void main(String[] args){
        LocalCacheImpl localCacheImpl = new LocalCacheImpl(1, 10, 4000, 25, 0.25f, "TestLocalCache");
        
        for (int i = 0; i < 1; i++) {
            new Thread(new PutCacheTask(localCacheImpl)).start();
        }
    }
    
    static class PutCacheTask implements Runnable{

        LocalCacheImpl localCacheImpl;
        
        public PutCacheTask(LocalCacheImpl localCacheImpl) {
            this.localCacheImpl = localCacheImpl;
        }
        
        public void run() {
            int count=4010;
            while (count>0) {
                localCacheImpl.put(String.valueOf(count), System.currentTimeMillis(), 5);
                
                if(count==11){
                    System.out.println("put 11");
                    try {
                        Thread.sleep(1*1000*60L+2000L);
                    } catch (InterruptedException e) {
                    }
                }
                
                if(count==6){
                    System.out.println("put 6");
                    try {
                        Thread.sleep(1*1000*60L+2000L);
                    } catch (InterruptedException e) {
                    }
                }
                
                if(count == 1){
                    System.out.println("put 1");
                    try {
                        Thread.sleep(1*1000*60L+2000L);
                    } catch (InterruptedException e) {
                    }
                }
                count--;
            }
        }
        
    }
}

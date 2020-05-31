package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Lock {
    public static final int SHARED_LOCK = 0;
    public static final int EXCLUSIVE_LOCK = 1;

    private final TransactionId tid;
    private int lockType;
    public Lock(TransactionId tid, int lockType){
        this.tid = tid;
        this.lockType = lockType;
    }
    public TransactionId getTid(){
        return this.tid;
    }
    public int getLockType(){
        return this.lockType;
    }
    public void setLockType(int lockType){
        this.lockType = lockType;
    }
}

class LockManager{
    ConcurrentHashMap<PageId, ArrayList<Lock>>lockMap;

    public LockManager(){
        this.lockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquiresLock(TransactionId tid, PageId pid, int lockType){
        if(lockMap.get(pid) == null){
            Lock lock = new Lock(tid, lockType);
            ArrayList<Lock> locks = new ArrayList<>();
            locks.add(lock);
            lockMap.put(pid, locks);
            return true;
        }
        ArrayList<Lock> locks = lockMap.get(pid);
        for (Lock lock:locks) {
            if(lock.getTid() == tid){
                if(lock.getLockType() == lockType)
                    return true;
                // lockType = EXCLUSIVE_LOCK, acquiresLock = SHARED_LOCK
                if(lock.getLockType() == Lock.EXCLUSIVE_LOCK) {
                    lock.setLockType(Lock.SHARED_LOCK);
                    return true;
                }
                // lockType = SHARED_LOCK, acquiresLock = EXCLUSIVE_LOCK
                else{
                    // If transaction t is the only transaction holding a shared lock on an object o,
                    // t may upgrade its lock on o to an exclusive lock.
                    if(locks.size() == 1){
                        lock.setLockType(Lock.EXCLUSIVE_LOCK);
                        return true;
                    }
                    return false;
                }
            }
        }
        // Only one transaction may have an exclusive lock on an object.
        if(locks.get(0).getLockType() == Lock.EXCLUSIVE_LOCK){
            return false;
        }
        else {
            if (lockType == Lock.SHARED_LOCK) {
                Lock lock = new Lock(tid, lockType);
                locks.add(lock);
                return true;
            }
            else {
                return false;
            }
        }
    }

    public synchronized void releasesLock(TransactionId tid, PageId pid){
        ArrayList<Lock> locks = lockMap.get(pid);
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if(lock.getTid() == tid){
                locks.remove(lock);
                if(locks.size() == 0)
                    lockMap.remove(pid);
                return;
            }
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        if(lockMap.get(pid) != null){
            ArrayList<Lock> locks = lockMap.get(pid);
            for (Lock lock:locks){
                if (lock.getTid() == tid)
                    return true;
            }
            return false;
        }
        return false;
    }
}

package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement

            for (Lock lock: locks) {
                if (lock.transactionNum.equals(except)) continue;
                if (!LockType.compatible(lockType, lock.lockType)){
                    return false;
                }

            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement

            boolean lockExists = false;

            if (!transactionLocks.containsKey(lock.transactionNum)){
                transactionLocks.put(lock.transactionNum, new ArrayList<>());
            }

            List<Lock> transactionLockList = transactionLocks.get(lock.transactionNum);

            for (Lock currLock: transactionLockList){
                if (currLock.name.equals(lock.name)){
                    currLock.lockType = lock.lockType; // update
                    return;
                }
            }

            transactionLockList.add(lock);
            resourceEntries.get(lock.name).locks.add(lock);


            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement

            // release/remove lock from locks
            if (locks.contains(lock)) {
                locks.remove(lock);
                //locks.remove(lock);
                List<Lock> transactionLockList = transactionLocks.get(lock.transactionNum);

                // from list of locks of transaction, remove particular lock
                transactionLockList.remove(lock);
                transactionLocks.put(lock.transactionNum, transactionLockList);

            }

            // process queue
            processQueue();

            return;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement

            if (addFront) waitingQueue.addFirst(request);
            else waitingQueue.addLast(request);
            return;

        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {

            synchronized (this) {


                Iterator<LockRequest> requests = waitingQueue.iterator();

                while (requests.hasNext()){
                    LockRequest request = requests.next(); // first request on waitingQueue
                    // check if first request is compatible with all locks on resource
                    if (locks.size() == 0) {
                        grantOrUpdateLock(request.lock);
                        // release request from queue
                        waitingQueue.removeFirst();
                        request.transaction.unblock();

                    }
                    else {
                        for (Lock currLock : locks) {
                            if (LockType.compatible(request.lock.lockType, currLock.lockType)) {
                                grantOrUpdateLock(request.lock);
                                promote(TransactionContext.getTransaction(), currLock.name, request.lock.lockType);
                                // release request from queue
                                waitingQueue.removeFirst();

                                for (Lock currReleasedLock : request.releasedLocks) {
                                    if (currReleasedLock.transactionNum.equals(request.lock.transactionNum)) {
                                        releaseLock(currReleasedLock);
                                    }
                                }
                                request.transaction.prepareBlock();
                                request.transaction.unblock();

                            }
                            if (currLock.transactionNum == request.lock.transactionNum){
                                if (LockType.substitutable(request.lock.lockType, currLock.lockType)){
                                    //promote(TransactionContext.getTransaction(), currLock.name, request.lock.lockType);
                                    // manually promote
                                    // update resource locks list
                                    locks.remove(currLock);
                                    locks.add(request.lock);
                                    waitingQueue.removeFirst();

                                    // update transactionLocks list
                                    List<Lock> transactionLockList = transactionLocks.get(currLock.transactionNum);

                                    int posCurrLock = 0;
                                    // find position of currLock in transactionLocks
                                    for (Lock currLockXract: transactionLockList){
                                        if (currLockXract.equals(currLock)) break;
                                        posCurrLock++;
                                    }
                                    transactionLockList.remove(posCurrLock);
                                    transactionLockList.add(posCurrLock, request.lock);
                                    //request.transaction.prepareBlock();
                                    request.transaction.unblock();

                                   // waitingQueue.removeFirst();


                                }
                            }

                            else {
                                return;
                            }
                        }
                    }
                }





                // check that the lockRequest does not clash with the existing requests
                // if lockRequest clashes, stop processing queue

                /*boolean lockClash = false;

                // TODO(proj4_part1): implement
                while (requests.hasNext() && !lockClash) {
                    LockRequest currLockReq = requests.next();

                    for (Lock currLock: locks) {
                        // check compatibility of requested locks with existing locks

                        if (LockType.compatible(currLockReq.lock.lockType, currLock.lockType)) {
                            // if compatible, grant lock
                            grantOrUpdateLock(currLockReq.lock);
                            currLockReq.transaction.unblock();
                        }
                        // stop when next lock cannot be granted
                        else {
                            lockClash = true;
                            break;
                        }

                    }
                    if (lockClash) break;
                }**/
            }
            //return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement

            for (Lock lock: locks){
                //getLockType(TransactionContext.getTransaction(), lock.name);
                if (lock.transactionNum == transaction) return lock.lockType;
            }

            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        ResourceEntry resourceEntry = getResourceEntry(name);
        Lock newLock = new Lock(name, lockType, transaction.getTransNum());
        LockRequest newReq = new LockRequest(transaction, newLock);
        boolean notComp = false;


        synchronized (this) {
            // error checking
            /*If
             * the new lock is not compatible with another transaction's lock on the
             * resource, the transaction is blocked and the request is placed at the
             * FRONT of the resource's queue.**/

            //* @throws DuplicateLockRequestException if a lock on `name` is already held
            //     * by `transaction` and isn't being released
            // check if name belongs in releaseName





            // check if name belongs in releaseName
            boolean nameBelongs = false;
            for (ResourceName currName: releaseNames){
                if (currName.equals(name)){
                    nameBelongs = true;
                }
            }
            // if name isn't being released
            if (!nameBelongs){
                // check if transaction holds lock on name
                for (Lock currLock: getLocks(name)){
                    if (currLock.transactionNum == transaction.getTransNum()){
                        // lock on name is already held by transaction
                        // throw duplicate error
                        throw new DuplicateLockRequestException("lock on `name` is already held by `transaction` and isn't being released");
                    }
                }
            }


            // check if newLock is substitutable with active lock
            for (Lock currLock: getLocks(name)){
                if (currLock.transactionNum == newLock.transactionNum){
                    if (LockType.substitutable(lockType, currLock.lockType)){

                        try {
                            promote(transaction, name, lockType);
                        }
                        catch (DuplicateLockRequestException | NoLockHeldException | InvalidLockException e){
                            throw e;
                        }

                        transaction.unblock();
                        return;

                    }
                }

            }


            if (!resourceEntry.checkCompatible(lockType, transaction.getTransNum())){
                transaction.prepareBlock();
                shouldBlock = true;
                resourceEntry.waitingQueue.addFirst(newReq);
                resourceEntry.processQueue();

            }

            // if compatible
            else{
                // let transaction acquire newLock on name
                resourceEntry.grantOrUpdateLock(newLock);


                if (releaseNames.size()!=0) {

                    for (ResourceName releaseName : releaseNames) {
                        ResourceEntry newResourceEntry = getResourceEntry(releaseName);

                        int countLock = 0;

                        // go through locks on newResourceEntry and release them if transaction has locks on newResourceEntry
                        for (Lock currLock: getLocks(releaseName)){
                            if (currLock.transactionNum == transaction.getTransNum()){
                                newResourceEntry.releaseLock(currLock);
                                countLock++;
                            }
                        }
                        // if transaction does not have one or more locks on newResourceEntry
                        if (countLock == 0) throw new NoLockHeldException("No lock held on this resource by transaction.");
                    }
                }
            }


        }
        if (shouldBlock) {
            transaction.block();
        }
        transaction.unblock();

    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.


        boolean shouldBlock = false;
        synchronized (this) {


            // from name get ResourceEntry

            ResourceEntry resourceEntry = getResourceEntry(name);

            // create lock of lockType
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, newLock);

            // error checking
            for (Lock currLock: getLocks(name)){
                if (currLock.transactionNum == newLock.transactionNum && !currLock.lockType.equals(LockType.NL)){
                    throw  new DuplicateLockRequestException("duplicate lock request by transaction.");
                }
            }

            if (resourceEntry.checkCompatible(lockType, transaction.getTransNum()) && resourceEntry.waitingQueue.isEmpty()){
                resourceEntry.grantOrUpdateLock(newLock);
            }
            else{
                resourceEntry.waitingQueue.addLast(request);
                transaction.prepareBlock();
                shouldBlock = true;
            }
            
        }
        if (shouldBlock) {
            transaction.block();
        }
        transaction.unblock();
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.

        ResourceEntry resourceEntry = getResourceEntry(name);

        synchronized (this) {

            // if there are no locks on resource, it automatically means that transaction does not hold any lock on it
            if (getLocks(name).size() == 0) throw new NoLockHeldException("No lock is held by transaction.");

            boolean lockNotHeld = true;



            for (Lock currLock: getLocks(name)){
                if (currLock.transactionNum == transaction.getTransNum()){

                    lockNotHeld = false;
                    resourceEntry.releaseLock(currLock);

                    return;



                }

            }

            // lock not held by transaction on resource
            if (lockNotHeld) throw new NoLockHeldException("No lock is held by transaction.");
            
        }

    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        ResourceEntry resourceEntry = getResourceEntry(name);
        Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
        LockRequest newLockReq = new LockRequest(transaction, newLock);

        synchronized (this) {


            // If the new lock
            // * is not compatible with another transaction's lock on the resource, the
            // * transaction is blocked and the request is placed at the FRONT of the
            // * resource's queue.

            int currLockIndex = -1;
            boolean xractNoLockOnName = true; // needed for NoLockHeldException
            //boolean promotionOccured = false;
            LockType currLockType;
            int sizeNameLocks = getLocks(name).size();

            // if new lock type is SIX, requests go to acquireAndRelease. Release redundant locks.
            // redundant locks: S(A) IS(A) IX(A)

            List<ResourceName> releaseNames = new ArrayList<>();

            // check which resources the transaction holds S, IS, IX locks on
            // also check if transaction holds lock on resource
            for (Lock xractLock: getLocks(transaction)){
                if (xractLock.name.equals(name)){
                    xractNoLockOnName = false;
                }
                if (newLockType.equals(LockType.SIX) && (xractLock.lockType.equals(LockType.S) || xractLock.lockType.equals(LockType.IS) || xractLock.lockType.equals(LockType.SIX))){
                    releaseNames.add(xractLock.name);
                }
            }
            if (xractNoLockOnName) throw new NoLockHeldException("transaction has no lock on name");
            if (newLockType.equals(LockType.SIX)){
                acquireAndRelease(transaction, name, newLockType, releaseNames);
            }


            if (!resourceEntry.checkCompatible(newLockType, newLock.transactionNum)){
                resourceEntry.waitingQueue.addLast(newLockReq);
                shouldBlock = true;
                transaction.prepareBlock();

            }

            else {

                // go thru all locks on resource and do error checking
                for (Lock currLock : getLocks(name)) {
                    currLockIndex += 1;


                    //* @throws DuplicateLockRequestException if `transaction` already has a
                    //* `newLockType` lock on `name`
                    if (currLock.lockType == newLockType && currLock.transactionNum == transaction.getTransNum())
                        throw new DuplicateLockRequestException(
                                "`transaction` already has a newLockType` lock on `name`");


                    // NOT SURE IF I SHOULD INCLUDE OR EXCLUDE BELOW COMMENTED CODE
                    /*if (!LockType.substitutable(newLockType, currLock.lockType)) {
                        // request is placed at the front of the resource's queue
                        resourceEntry.waitingQueue.addFirst(new LockRequest(transaction, newLock));
                        shouldBlock = true;
                        transaction.prepareBlock();
                        break;
                    }**/


                    // find currLockType of transaction on resource
                    if (currLock.transactionNum == transaction.getTransNum()) {
                        currLockType = currLock.lockType;
                        // * Promote a transaction's lock on `name` to `newLockType` (i.e. change
                        // * the transaction's lock on `name` from the current lock type to
                        // * `newLockType`, if its a valid substitution).
                        if (LockType.substitutable(newLockType, currLockType) && currLockType != newLockType) {
                            /** change the transaction's lock on `name` from the current lock type to`newLockType` **/

                            /** update locks list of resource; replace old lock with new lock**/
                            resourceEntry.locks.remove(currLockIndex); // remove lock at index
                            resourceEntry.processQueue();

                            resourceEntry.locks.add(currLockIndex, newLock);

                            /** update transactionLocks **/
                            List<Lock> transactionLockList = transactionLocks.get(transaction.getTransNum());
                            // get index of currLock on list
                            int currLockIndexTransaction = 0;
                            for (int i = 0; i < transactionLockList.size(); i++) {
                                if (transactionLockList.get(i) == currLock) currLockIndexTransaction = i;
                            }

                            // remove lock of transaction at index currLockIndexTransaction
                            transactionLockList.remove(currLockIndexTransaction);
                            //put new lock
                            transactionLockList.add(currLockIndexTransaction, newLock);
                            transactionLocks.put(transaction.getTransNum(), transactionLockList);

                            break;

                        }
                        else if (LockType.compatible(newLockType, currLockType)){
                            /** update locks list of resource; replace old lock with new lock**/
                            resourceEntry.locks.remove(currLockIndex); // remove lock at index
                            resourceEntry.processQueue();

                            resourceEntry.locks.add(currLockIndex, newLock);

                            /** update transactionLocks **/
                            List<Lock> transactionLockList = transactionLocks.get(transaction.getTransNum());
                            // get index of currLock on list
                            int currLockIndexTransaction = 0;
                            for (int i = 0; i < transactionLockList.size(); i++) {
                                if (transactionLockList.get(i) == currLock) currLockIndexTransaction = i;
                            }

                            // remove lock of transaction at index currLockIndexTransaction
                            transactionLockList.remove(currLockIndexTransaction);
                            //put new lock
                            transactionLockList.add(currLockIndexTransaction, newLock);
                            transactionLocks.put(transaction.getTransNum(), transactionLockList);

                            break;

                        }

                        else throw new InvalidLockException("requested lock type is not a promotion");

                    }


                }
            }

            //* @throws NoLockHeldException if `transaction` has no lock on `name`
            if (xractNoLockOnName) throw  new NoLockHeldException("`transaction` has no lock on `name`");


        }
        if (shouldBlock) {
            transaction.block();
        }
        else transaction.unblock();
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);

        //if (getLocks(name).size() != 0){
            // go thru all locks and check if lock.transactionNum == transaction.transactionNum
            for (Lock currLock: getLocks(name)){
                if (currLock.transactionNum == transaction.getTransNum()){
                    return currLock.lockType;
                }
            }
        //}

        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}

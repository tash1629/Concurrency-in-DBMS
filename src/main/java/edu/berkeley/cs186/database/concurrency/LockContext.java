package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        if (readonly) throw new UnsupportedOperationException();

        if (parent==null){ // if current context is database // no need to check for canBeParentLock
            ResourceName currName = getResourceName();
            List<ResourceName> releaseNames = new ArrayList<>();
            releaseNames.add(getResourceName());
            //lockman.acquireAndRelease(transaction, getResourceName(), lockType, releaseNames);
            lockman.acquire(transaction, getResourceName(), lockType);


            return;

        }

        // check if parent context has correct intent lock
        LockType parentLockType = parent.getExplicitLockType(transaction);
        if (parent != null && !LockType.canBeParentLock(parentLockType, lockType)){
            throw new InvalidLockException("locktype is not compatible with parent lock");
        }
        if (parent != null && LockType.canBeParentLock(parentLockType, lockType)){
            // acquire lock
            lockman.acquire(transaction, getResourceName(), lockType);
            // numChildLocks of the parent context should be updating because its child (the current LockContext) is acquiring/releasing a lock.

            int currNumChildLocks = parent.getNumChildren(transaction);
            currNumChildLocks++;
            parent.numChildLocks.put(transaction.getTransNum(), currNumChildLocks);

        }


        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        if (readonly) throw new UnsupportedOperationException();


        // get all locks held by transaction
        List<Lock> locks = lockman.getLocks(transaction); // list of locks held by transaction

        // if a transaction lock is on a descendent resource, it is a child lock
        // if any child lock needs parent lock (current context lock) to still hold its lock, current context cant release its lock
        for (Lock currLock: locks){

            // if a transaction lock is on a descendent resource, it is a child lock
            if (currLock.name.isDescendantOf(getResourceName())){
                if (!currLock.lockType.equals(LockType.NL)) throw new InvalidLockException("Child lock messed with");
            }
        }

        // if no problem releasing current context lock, release lock

        lockman.release(transaction, getResourceName());
        if (parent !=null) {
            int currNumChildLocks = parent.getNumChildren(transaction);
            currNumChildLocks--;
            parent.numChildLocks.put(transaction.getTransNum(), currNumChildLocks);
        }
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        if (readonly) throw  new UnsupportedOperationException();

        // check for InvalidLockException
        LockType currLockTypeOnContext = getExplicitLockType(transaction);
        if (!LockType.substitutable(newLockType, currLockTypeOnContext)){ // !currLockTypeOnContext.equals(LockType.NL) &&
            throw new InvalidLockException("Not promotable to newLockType");
        }
        if (newLockType.equals(currLockTypeOnContext)){
            throw new InvalidLockException("Not promotable to newLockType");
        }

        // if B is SIX and A is IS/IX/S
        if (newLockType.equals(LockType.SIX)){
            if (!(currLockTypeOnContext.equals(LockType.IS) || currLockTypeOnContext.equals(LockType.IX) || currLockTypeOnContext.equals(LockType.S))){
                throw new InvalidLockException("Not promotable to newLockType");
            }
            else if (hasSIXAncestor(transaction)) throw  new InvalidLockException("Not promotable to newLockType");
            else{
                List<ResourceName> releaseNames = sisDescendants(transaction);
                lockman.acquireAndRelease(transaction, getResourceName(), LockType.SIX,releaseNames);
            }
        }
        else{
            lockman.promote(transaction, getResourceName(), newLockType);
        }



        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement

        if (readonly) throw new UnsupportedOperationException();

        // get locks held by transaction
        List<Lock> locks = lockman.getLocks(transaction);

        LockType currContextLockType = getExplicitLockType(transaction);

        //check if transaction has locks at this level
        if (currContextLockType.equals(LockType.NL)) throw new NoLockHeldException("Transaction holds no lock at this level");


        /* start escalate process*/

        Lock currContextLock = new Lock(getResourceName(), currContextLockType, transaction.getTransNum());
        int indexOfCurrContextLockType = locks.indexOf(currContextLock);

        // check if context has children
        boolean yesChild = false;

        // get list of childLocks
        List<ResourceName> childLockNames = new ArrayList<>();
        LockContext lockContextOfChild;

        // go through all children
        for (Lock currLock: locks){

            if (currLock.name.isDescendantOf(getResourceName())){ // currlock is a child lock
                childLockNames.add(currLock.name);
                yesChild = true;
                // if current level lock is an intent lock
                if (currContextLockType.isIntent()){
                    // if currContextLockType is IX

                    if (currContextLockType.equals(LockType.IX)){
                        // look for finer X lock
                        if (currLock.lockType.equals(LockType.X)){

                            // ACQUIREANDRELEASE
                            List<ResourceName> releaseNames = new ArrayList<>();
                            releaseNames.add(getResourceName());
                            lockman.acquireAndRelease(transaction, getResourceName(), LockType.X, releaseNames);
                            /*  START
                            // remove IX lock, acquire X lock

                            // releaseLock on transaction
                            lockman.release(transaction, getResourceName());
                            // acquire new lock
                            acquire(transaction, LockType.X);
                            END
                             */

                        }

                        // No finer X Lock, replace with S lock
                        // remove IX lock, acquire S lock

                        //USING ACQUIRE AND RELEASE
                        /*List<ResourceName> releaseNames = new ArrayList<>();
                        releaseNames.add(getResourceName());
                        lockman.acquireAndRelease(transaction, getResourceName(), LockType.S, releaseNames);**/


                        // releaseLock on transaction
                        lockman.release(transaction, getResourceName());
                        // acquire new lock
                        acquire(transaction, LockType.S);

                    }

                    if (currContextLockType.equals(LockType.IS)){


                        // using acquireandrelease
                        /*List<ResourceName> releaseNames = new ArrayList<>();
                        releaseNames.add(getResourceName());
                        lockman.acquireAndRelease(transaction, getResourceName(), LockType.S, releaseNames);**/



                        // releaseLock on transaction
                        lockman.release(transaction, getResourceName());
                        // acquire new lock
                        acquire(transaction, LockType.S);




                    }

                    if (currContextLockType.equals(LockType.SIX)){
                        // check if children have X lock
                        if (currLock.lockType.equals(LockType.X)){

                            lockman.promote(transaction, getResourceName(), LockType.X);
                            /*// releaseLock on transaction
                            START
                            lockman.release(transaction, getResourceName());
                            // acquire new lock
                            acquire(transaction, LockType.X); END**/
                        }

                        // No finer X Lock, replace with S lock
                        // remove SIX lock, still acquire X lock
                        lockman.promote(transaction, getResourceName(), LockType.X);
                        /*START
                        // releaseLock on transaction
                        lockman.release(transaction, getResourceName());
                        // acquire new lock
                        acquire(transaction, LockType.X); END**/

                    }

                }
            }

        }

        if (!yesChild){ // no child
            // replace IS with S at this level
            if (currContextLockType.equals(LockType.IS)){

                // releaseLock on transaction
                lockman.release(transaction, getResourceName());
                // acquire new lock
                acquire(transaction, LockType.S);

                return;
            }

            // replace IX with X at this level
            if (currContextLockType.equals(LockType.IX)){
                // releaseLock on transaction
                lockman.release(transaction, getResourceName());
                // acquire new lock
                acquire(transaction, LockType.X);

                return;
            }


        }



        else {
            // nullify all children locks
            int indexChildLock = 0;
            for (Lock currLock1 : locks) {
                if (currLock1.name.isDescendantOf(getResourceName())) {
                    // Nullify child lock in transactionLocks
                    // releaseLock on resource
                    lockman.release(transaction, currLock1.name);

                    ResourceName currLockName = currLock1.name;
                    String strCurrLockName = currLockName.toString();

                    lockContextOfChild = fromResourceName(lockman, currLockName);
                    // acquire null lock\ lock on childContext

                    lockContextOfChild.acquire(transaction, LockType.NL);

                }
                indexChildLock++;
            }
            // update numChildLocks
            int decrementBy = getNumChildren(transaction);
            int numChildLocksOfThisLevel = getNumChildren(transaction);;
            numChildLocks.put(transaction.getTransNum(), numChildLocksOfThisLevel-decrementBy);
        }



        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
        //return LockType.NL;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        /*
        loop through the parents until parent is null. When parent is null, get the lockType on that level. The locktype on children should be the same as the parent.
        keep track of parent using currParent = parent.parent
        while (currParent != null){
            keep changing currParent
        }
        finally get lock type of parent
         */

        if (!getExplicitLockType(transaction).equals(LockType.NL)) return getExplicitLockType(transaction);

        // else
        LockContext currNode = fromResourceName(lockman, getResourceName());
        LockContext currParent = currNode.parent;


        LockType parentLockType = currNode.parent.getExplicitLockType(transaction);

        while (!(currNode.getExplicitLockType(transaction).equals(null))){
            if (currNode.getExplicitLockType(transaction) != LockType.NL) break;
            if (currNode.parent != null) currNode = currNode.parent;
            if (currNode.parent == null) break;
        }


        // now that parent of currParent lockType is NL,
        if (!(currNode.getExplicitLockType(transaction).equals(LockType.IS) || currNode.getExplicitLockType(transaction).equals(LockType.IX))){
            LockType currLockType = currNode.getExplicitLockType(transaction);
            return currLockType;
        }
        else return LockType.NL;





    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // go through lockContext parents and check if transaction holds SIX lock
        LockContext currContext = fromResourceName(lockman, getResourceName());

        while (currContext != null){
            if (currContext.getExplicitLockType(transaction).equals(LockType.SIX)) return true;
            currContext = currContext.parent;
        }

        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> releaseNames = new ArrayList<>();
        // go through all transaction locks and check if its a descendent lock
        for (Lock lock: lockman.getLocks(transaction)){
            if (lock.name.isDescendantOf(getResourceName())){
                if (lock.lockType.equals(LockType.S) || lock.lockType.equals(LockType.IS)){
                    releaseNames.add(lock.name);
                }

            }
        }

        return releaseNames;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}


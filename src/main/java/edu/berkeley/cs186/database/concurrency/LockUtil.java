package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor locks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;


        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

        // find how many parents lockContext has
        int numParents = 0;
        LockContext currContext = lockContext;
        while (currContext.parent != null){
            currContext = currContext.parent;
            numParents++;
        }




        if (requestType.equals((LockType.S))){

            // handle promotion
            if (LockType.substitutable(LockType.S, explicitLockType)){
                List<ResourceName> releaseNames = new ArrayList<>();
                releaseNames.add(lockContext.getResourceName());
                lockContext.lockman.acquireAndRelease(transaction, lockContext.getResourceName(), LockType.S, releaseNames);
            }

            // handle escalation
            // escalate when there are SIS descendents
            for (Lock currLock : lockContext.lockman.getLocks(transaction)){
                if (currLock.name.isDescendantOf(lockContext.getResourceName())){
                    if (currLock.lockType.equals(LockType.S) || currLock.lockType.equals(LockType.IS)){
                        lockContext.escalate(transaction);
                        break;
                    }
                }
            }

            // handle acquisition
            if ((LockType.compatible(requestType, lockContext.getExplicitLockType(transaction)) || lockContext.getExplicitLockType(transaction).equals(LockType.NL))) {

                // ensure ancestors have correct locks
                LockType parentLockType = lockContext.getEffectiveLockType(transaction);

                // acquire IS lockType on parents until parent lockType != NL
                // we acquire locks from top down. Need to reach node whose parent node is not null


                LockContext currNode = lockContext;

                // acquire IS lock on all of lockContext parents
                for (int i = 0; i < numParents; i++) {

                    while (!(currNode.getExplicitLockType(transaction).equals(null))) {
                        if (currNode.getExplicitLockType(transaction) != LockType.NL) break;
                        if (currNode.parent.getExplicitLockType(transaction) != LockType.NL) break;
                        if (currNode.parent != null) currNode = currNode.parent;
                        if (currNode.parent == null) break;
                    }
                    // acquire IS lock on current parent
                    if (!currNode.getExplicitLockType(transaction).equals(LockType.IS))
                        currNode.acquire(transaction, LockType.IS);
                    currNode = lockContext;

                }
                // acquire requestType on lockContext if lockContext does not have any lock on it
                if (lockContext.getExplicitLockType(transaction).equals(LockType.NL))
                    lockContext.acquire(transaction, LockType.S);

                    // else release and acquire
                else {
                    //lockContext.lockman.promote(transaction, lockContext.getResourceName(), LockType.S);

                    List<ResourceName> releaseNames = new ArrayList<>();
                    releaseNames.add(lockContext.getResourceName());


                    // release locks that transaction holds on every other resource
                    lockContext.lockman.acquireAndRelease(transaction, lockContext.getResourceName(), LockType.S, releaseNames);

                }

                return;
            }
        }



        // handle promotion
        else if (requestType.equals(LockType.X) && (LockType.substitutable(requestType, lockContext.getExplicitLockType(transaction)))){
            // if substitutable, promote lock context lock to requestType

            // check what locks lockContext parents hold and substitute for IX

            LockContext currNode = lockContext;

            // promote locks on all of lockContext parents to IX

            for (int i=0; i<numParents; i++) { // one loop promotes database, second loop promotes table
                while (currNode != null) {
                    // on lock context level, meaning not a parent
                    if (lockContext.name.equals(currNode.getResourceName())) {
                        currNode = currNode.parent;
                        continue;
                    }
                    // currNode is a parent
                    // make sure only database enters
                    else if (LockType.substitutable(LockType.IX, currNode.getExplicitLockType(transaction)) && currNode.parent == null) {
                        currNode.promote(transaction, LockType.IX);
                        currNode = currNode.parent;
                        break;
                    }
                    // make sure only table enters
                    else if (currNode.parent.getExplicitLockType(transaction).equals(LockType.IX) && LockType.substitutable(LockType.IX, currNode.getExplicitLockType(transaction))) {
                        currNode.promote(transaction, LockType.IX);
                        currNode = currNode.parent;
                        break;
                    }
                    currNode = currNode.parent;
                }
                // reset currNode to lockContext
                currNode = lockContext;

            }



            // promote lockContext lock to X
            lockContext.promote(transaction, LockType.X);
            return;


        }



    }

    // TODO(proj4_part2) add any helper methods you want


}

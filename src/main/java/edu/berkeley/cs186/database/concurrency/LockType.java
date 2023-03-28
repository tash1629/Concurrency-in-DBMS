package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        // NL
        if ((a==NL && b == S) || (a==S && b == NL)) return true;
        if ((a==NL && b == X) || (a==X && b == NL)) return true;
        if ((a==NL && b == IX) || (a==IX && b == NL)) return true;
        if ((a==NL && b == SIX) || (a==SIX && b == NL)) return true;
        if ((a==NL && b == IS) || (a==IS && b == NL)) return true;
        if (a==NL && b == NL) return true;


        // lock compatibility of a=S or b=S with the other types of locks
        if (a == S && b == S) return true;
        if ((a==S && b==IS) || (a ==IS && b==S)) return true;
        if ((a==S && b==IX) || (a ==IX && b==S)) return false;
        if ((a==S && b==SIX) || (a == SIX && b==S)) return false;
        if ((a==S && b==X) || (a ==X && b==S)) return false;

        // X
        if ((a==X && b == S) || (a==S && b == X) ) return false;
        if ((a==X && b == IS) || (a==IS && b == X) ) return false;
        if ((a==X && b == IX) || (a==IX && b == X) ) return false;
        if ((a==X && b == SIX) || (a==SIX && b == X) ) return false;
        if (a==X && b == X) return false;

        // IS
        if ((a==IS && b == S) || (a==S && b == IS) ) return true;
        if ((a==IS && b == IX) || (a==IX && b == IS) ) return true;
        if ((a==IS && b == SIX) || (a==SIX && b == IS) ) return true;
        if ((a==IS && b == X) || (a==X && b == IS) ) return false;
        if (a==IS && b == IS) return true;

        //IX
        if ((a==IX && b == IS) || (a==IS && b == IX) ) return true;
        if ((a==IX && b == S) || (a==S && b == IX) ) return false;
        if ((a==IX && b == SIX) || (a==SIX && b == IX) ) return false;
        if ((a==IX && b == X) || (a==X && b == IX) ) return false;
        if (a==IX && b == IX) return true;

        // SIX
        if ((a==SIX && b == IS) || (a==IS && b == SIX) ) return true;
        if ((a==SIX && b == S) || (a==S && b == SIX) ) return false;
        if ((a==SIX && b == IX) || (a==IX && b == SIX) ) return false;
        if ((a==SIX && b == X) || (a==X && b == SIX) ) return false;
        if (a==SIX && b == SIX) return false;



        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        if ((parentLockType == IS || parentLockType == IX) && (childLockType == IS || childLockType==S)) return true;
        if ((parentLockType == IX || parentLockType == SIX || parentLockType == X) && (childLockType == IX || childLockType==SIX)) return true;

        // any lock type can be parent of NL
        if ((parentLockType == X || parentLockType == S || parentLockType == IS || parentLockType == IX || parentLockType == SIX || parentLockType == NL) && (childLockType == NL)) return true;

        // IX can be parent of any lock type
        if ((parentLockType == IX) && (childLockType == X || childLockType == S || childLockType == IS || childLockType == NL || childLockType == SIX))  return true;

        // IS can be the parent of IS, S, and NL
        if ((parentLockType == IS) && (childLockType == IS || childLockType==S || childLockType==NL)) return true;


        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        // You can't substitute anything with NL, other than NL
        if (required == NL && substitute == NL) return  true;

        // You can substitute S with S, SIX, or X
        if (required == S && (substitute == S || substitute == SIX || substitute == X)) return true;

        // You can substitute X with X
        if (required == X && substitute == X) return true;

        // You can substitute intent locks with themselves
        if (required == IS && substitute == IS) return true;
        if (required == IS && substitute == IX) return true;

        // IX's privileges are a superset of IS's privileges
        if (required == IX && substitute == IX) return true;

        // IX's privileges are a superset of IS's privileges
        //if (required == IX && substitute == IS ) return true;



        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}


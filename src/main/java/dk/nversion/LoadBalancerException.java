package dk.nversion;

import java.lang.Exception;import java.lang.String; /**
 * Created by tlb on 8/9/15.
 */
public class LoadBalancerException extends Exception {
    public LoadBalancerException(String message) {
        super(message);
    }
}

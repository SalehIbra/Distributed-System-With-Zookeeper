package cluster.management;

//since our programming model is event driven
//we need a way to trigger different events based on whether the current node was elected to be a leader
//or it became a worker.
//To keep leader election and service registry separate, we will integrate both using a callback.

import org.apache.zookeeper.KeeperException;

// After every leader election only one of these methods will be called
public interface OnElectionCallback {
    void onElectedToBeLeader() throws KeeperException, InterruptedException;

    void onWorker();
}

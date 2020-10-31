package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
// Zookeeper client api is a synchronous and event driven

// to be notified about events such a successful connection or disconnection we need to
// register an event handler and pass it to zookeeper as watcher object (so we need to implement Watcher
// method to be watcher)
public class LeaderElection implements Watcher {

    private static final String ELECTION_NAMESPACE = "/election";
    private String currectZnodeName;
    private ZooKeeper zooKeeper;
    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper ,OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        // create znode from sequence and delete when disconnect, return znode full path
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name" + znodeFullPath);
        // extract the name only
        this.currectZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");

    }

    public void reElectLeader() throws KeeperException, InterruptedException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            // get the children znode of the current znode in path
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);
            if (smallestChild.equals(currectZnodeName)) {
                System.out.println("I am the leader");
                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                System.out.println("I am not the leader");
                // get the predecessor node and watch it to reelect or fill the gap if the node deleted
                int predecessorIndex = Collections.binarySearch(children, currectZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        onElectionCallback.onWorker();
        System.out.println("Watching znode" + predecessorZnodeName);
        System.out.println();


    }


    // this process is called by zookeeper library on a separate thread whenever there is a new event
    // coming from the ZK server
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }
}

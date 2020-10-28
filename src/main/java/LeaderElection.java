import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
// Zookeeper client api is a synchronous and event driven

// to be notified about events such a successful connection or disconnection we need to
// register an event handler and pass it to zookeeper as watcher object (so we need to implement Watcher
// method to be watcher)
public class LeaderElection implements Watcher {
    // our client address from the conf
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String ELECTION_NAMESPACE = "/election";
    // object for all communication with the zookeeper server
    private ZooKeeper zooKeeper;
    // server will consider the client disconnected if didn't hear anything within 3 secs
    private static final int SESSION_TIMEOUT = 3000;
    private String currectZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application ");

    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE+"/c_";
        // create znode from sequence and delete when disconnect, return znode full path
        String znodeFullPath = zooKeeper.create(znodePrefix,new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name"+ znodeFullPath);
        // extract the name only
        this.currectZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE+"/","");

    }

    public void electLeader() throws KeeperException, InterruptedException {
        // get the children znode of the current znode in path
        List<String> children= zooKeeper.getChildren(ELECTION_NAMESPACE,false);
        Collections.sort(children);
        String smallestChild = children.get(0);
        if(smallestChild.equals(currectZnodeName)){
            System.out.println("I am the leader");
            return;
        }
        System.out.println("I am not hte leader,"+smallestChild+" is the leader");

    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT,this);
    }
    public void run() throws InterruptedException {
        // put main thread in wait to see event thread state
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    // this process is called by zookeeper library on a separate thread whenever there is a new event
    // coming from the ZK server
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
            case None:
                if(event.getState()== Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    // if another event (disconnect) , wake up main thread to release resources and exit
                    System.out.println("Disconnected from Zookeeper event");
                    synchronized (zooKeeper){
                        zooKeeper.notifyAll();
                    }
                }
        }
    }
}

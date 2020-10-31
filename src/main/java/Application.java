import cluster.management.LeaderElection;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    // our client address from the conf
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    // object for all communication with the zookeeper server
    private ZooKeeper zooKeeper;
    // server will consider the client disconnected if didn't hear anything within 3 secs
    private static final int SESSION_TIMEOUT = 3000;
    // for running in different machines they can all use this port since the address is different
    private static final int DEFAULT_PORT = 8080;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // If the port was not passed into the application as argument use the default
        int curentServerPort = args.length == 1 ? Integer.parseInt(args[0]) :DEFAULT_PORT;
        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();

        ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);
        OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry,curentServerPort);

        LeaderElection leaderElection = new LeaderElection(zooKeeper,onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();

        application.run();
        application.close();
        System.out.println("Disconnected from Zookeeper, exiting application ");

    }

    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void run() throws InterruptedException {
        // put main thread in wait to see event thread state
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    // if another event (disconnect) , wake up main thread to release resources and exit
                    System.out.println("Disconnected from Zookeeper event");
                    synchronized (zooKeeper) {
                        zooKeeper.notifyAll();
                    }
                }
                break;

        }
    }
}

package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode = null;
    // store all nodes in the cluster (to avoid calling getChildren every time
    // we need a list of addresses)
    private List<String> allServiceAddresses = null;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistry();
    }
    // joining the cluster, adding our address to service registry
    public void registerToCluster(String metedata) throws KeeperException, InterruptedException {
        this.currentZnode = zooKeeper.create(REGISTRY_ZNODE+"/n_",metedata.getBytes(),
                              ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry");
    }

    // initial update addresses calling to get list of cluster members
    public void registerForUpdates(){
        try {
            updateAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    // unregister from the cluster(node shutdown itself or worker become a leader)
    public void unregisterFromCluster() throws KeeperException, InterruptedException {
        if(this.currentZnode != null && zooKeeper.exists(this.currentZnode,false) != null){
            zooKeeper.delete(currentZnode,-1);
        }
    }
    // to get most up to date addresses (cashed result)
    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if(allServiceAddresses == null){
            updateAddresses();
        }
        return allServiceAddresses;
    }


    private void createServiceRegistry(){
            try {
                if(zooKeeper.exists(REGISTRY_ZNODE,false) == null)
                zooKeeper.create(REGISTRY_ZNODE,new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException e) {
                // KeeperException solves condition race, when two nodes checks and then creates simultaneously
                // the second create throws this exception
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

    }
    //get list of addresses of members and
    // getting updates from the service registry about nodes joining and leaving the cluster
    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE,this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for (String workerZnode: workerZnodes){
            String workerZnodeFullPath = REGISTRY_ZNODE+"/"+workerZnode;
            //This code to solve condition race if a node deleted after getChildren and before exists
            Stat stat = zooKeeper.exists(workerZnodeFullPath,false);
            if(stat == null){
                continue;
            }
            byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
            addresses.add(new String(addressBytes));
        }
        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are : "+ this.allServiceAddresses);
    }

    // update our service addresses in case of changes and re-register for future updates
    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

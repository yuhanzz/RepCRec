import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
public class Database {
    int time;
    TransactionManager transactionManager;
    Map<Integer, Site> sites;

    /**
     * initialize the sites and the transaction manager
     */
    public void init() {
        time = 0;
        // initialize the sites
        for (int i = 1; i <= 10; i++) {
            sites.put(i, new Site(i));
        }
        // initialize the transaction manager
        transactionManager = new TransactionManager(sites);
    }

    /**
     * 
     */
    public void simulate(File inputFile) throws FileNotFoundException {
        // initialize scanner
        Scanner scanner;
        if (inputFile == null) {
            scanner = new Scanner(System.in);
        } else {
            scanner = new Scanner(inputFile);
        }

        // parse and execute each line (site.fail / site.recover / transactionManager.read)
        while (true) {

            // parse the line

            // site fail => call site.fail

            // site recover => 1. call site.recover 2. call transactionManager.receiveRecoverNotice(siteId)

            // other operations => transactionManager.handleRequest(operation)
        }

        // shut down everything
    }

    /**
     * initialize the database and start the simulation according to command line options
     */
    public static void main(String[] args) {
        // init the database by this.init

        // start the simulate
    }
    
}

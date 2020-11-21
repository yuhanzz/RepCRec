public class Database {
    TransactionManager transactionManager;
    Map<Integer, Site> sites;

    /**
     * initialize the sites and the transaction manager
     */
    public void init() {
        // initialize the sites

        // initialize the transaction manager
    }

    /**
     * 
     */
    public void simulate(File inputFile)
    {
        // initialize scanner
        Scanner scanner;
        if (inputFile == null) {
            scanner = new Scanner(System.in);
        } else {
            scanner = new Scanner(inputFile);
        }

        // parse and execute each line (site.fail / site.recover / transactionManager.read)
        while () {

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

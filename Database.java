import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Database {
    int time;
    TransactionManager transactionManager;
    Map<Integer, Site> sites;
    OutputPrinter outputPrinter;

    /**
     * initialize the sites and the transaction manager
     */
    public Database(boolean verbose) {
        time = 0;
        outputPrinter = new OutputPrinter(verbose);
        // initialize the sites
        for (int i = 1; i <= 10; i++) {
            sites.put(i, new Site(i, outputPrinter));
        }
        // initialize the transaction manager
        transactionManager = new TransactionManager(sites, outputPrinter);
    }

    public void dump() {
        for (int i = 1; i <= 10; i++) {
            Site site = sites.get(i);
            site.dump();
        }
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

        // parse and execute each line
        while (scanner.hasNextLine()) {
            if (transactionManager.deadLockDetection()) {
                transactionManager.retry(time);
            }

            // parse the line
            String line = scanner.nextLine();
            Matcher matcher = Pattern.compile("\\d+").matcher(line);

            if (line.contains("beginRO")) {
                int transactionId = Integer.valueOf(matcher.group());
                Operation operation = new Operation(OperationType.BEGIN_READ_ONLY, transactionId, time);
                transactionManager.handleNewRequest(operation, time);
            } else if (line.contains("begin")) {
                int transactionId = Integer.valueOf(matcher.group());
                Operation operation = new Operation(OperationType.BEGIN, transactionId, time);
                transactionManager.handleNewRequest(operation, time);
            } else if (line.contains("recover")) {
                int siteId = Integer.valueOf(matcher.group());
                Site site = sites.get(siteId);
                site.recover();
                transactionManager.retry(time);
            } else if (line.contains("fail")) {
                int siteId = Integer.valueOf(matcher.group());
                Site site = sites.get(siteId);
                site.fail();
            } else if (line.contains("end")) {
                int transactionId = Integer.valueOf(matcher.group());
                Operation operation = new Operation(OperationType.COMMIT, transactionId, time);
                transactionManager.handleNewRequest(operation, time);
            } else if (line.contains("dump")) {
                dump();
            } else if (line.contains("R")) {
                int transactionId = Integer.valueOf(matcher.group());
                int variableId = Integer.valueOf(matcher.group());
                Operation operation = new Operation(OperationType.READ, transactionId, variableId, time);
                transactionManager.handleNewRequest(operation, time);
            } else if (line.contains("W")) {
                int transactionId = Integer.valueOf(matcher.group());
                int variableId = Integer.valueOf(matcher.group());
                int value = Integer.valueOf(matcher.group());
                Operation operation = new Operation(OperationType.WRITE, transactionId, variableId, value, time);
                transactionManager.handleNewRequest(operation, time);
            }

            time++;
        }
    }
    
}

package src.main.java;

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
     * @param verbose adding additional information for debugging purposes
     */
    public Database(boolean verbose) {
        time = 0;
        sites = new HashMap<>();
        outputPrinter = new OutputPrinter(verbose);
        // initialize the sites
        for (int i = 1; i <= 10; i++) {
            sites.put(i, new Site(i, outputPrinter));
        }
        // initialize the transaction manager
        transactionManager = new TransactionManager(sites, outputPrinter);
    }

    /**
     * Dumping all the site's information
     */
    public void dump() {
        for (int i = 1; i <= 10; i++) {
            Site site = sites.get(i);
            site.dump();
        }
    }

    /**
     * parses each line of the input file and does the corresponding commands 
     * (e.g. beginRO, begin, recover, fail, end, dump, read, write)
     * @param inputFile the file with the commands 
     * @throws FileNotFoundException
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
            time++;

            if (transactionManager.deadLockDetection()) {
                transactionManager.retry(time);
            }

            // parse the line
            String line = scanner.nextLine();

            Pattern pattern = Pattern.compile("[0-9]+");
            List<Integer> numbers = new ArrayList<>();
            Matcher matcher = pattern.matcher(line);
            while (matcher.find()) {
                numbers.add(Integer.valueOf(matcher.group()));
            }

            if (line.contains("beginRO")) {
                int transactionId = numbers.get(0);
                Operation operation = new Operation(OperationType.BEGIN_READ_ONLY, transactionId, time);
                transactionManager.handleNewRequest(operation, time);
            } else if (line.contains("begin")) {
                int transactionId = numbers.get(0);
                Operation operation = new Operation(OperationType.BEGIN, transactionId, time);
                transactionManager.handleNewRequest(operation, time);
            } else if (line.contains("recover")) {
                int siteId = numbers.get(0);
                Site site = sites.get(siteId);
                site.recover();
                transactionManager.retry(time);
            } else if (line.contains("fail")) {
                int siteId = numbers.get(0);
                Site site = sites.get(siteId);
                site.fail();
                transactionManager.receiveFailureNotice(siteId, time);
            } else if (line.contains("end")) {
                int transactionId = numbers.get(0);
                Operation operation = new Operation(OperationType.COMMIT, transactionId, time);
                transactionManager.handleNewRequest(operation, time);
                transactionManager.retry(time);
            } else if (line.contains("dump")) {
                dump();
            } else if (line.contains("R")) {
                int transactionId = numbers.get(0);
                int variableId = numbers.get(1);
                Operation operation = new Operation(OperationType.READ, transactionId, variableId, time);
                transactionManager.handleNewRequest(operation, time);
            } else if (line.contains("W")) {
                int transactionId = numbers.get(0);
                int variableId = numbers.get(1);
                int value = numbers.get(2);
                Operation operation = new Operation(OperationType.WRITE, transactionId, variableId, value, time);
                transactionManager.handleNewRequest(operation, time);
            } else {
                break;
            }

        }
        outputPrinter.print();
    }
    
}

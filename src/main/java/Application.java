package src.main.java;

import java.io.File;
import java.io.FileNotFoundException;

public class Application {
    public static void main(String[] args) {
        boolean verbose = false;
        if (args.length > 0 && args[args.length - 1].equals("-v")) {
            verbose = true;
        }
        Database db = new Database(verbose);
        try {
            if (args.length == 1) {
                db.simulate(new File(args[0]));
            } else {
                db.simulate(null);
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found");
        }
    }
}

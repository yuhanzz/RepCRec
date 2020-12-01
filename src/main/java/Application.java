package src.main.java;

import java.io.File;
import java.io.FileNotFoundException;

public class Application {
    public static void main(String[] args) {
        boolean verbose = false;
        String fileName = null;
        for (String arg : args) {
            if (arg.equals("-v")) {
                verbose = true;
            } else {
                fileName = arg;
            }
        }
        Database db = new Database(verbose);
        try {
            if (fileName != null) {
                System.out.println(fileName);
                db.simulate(new File(fileName));
            } else {
                db.simulate(null);
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found");
        }
    }
}


package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("WordCount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("CountingTree", CountingTree.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("DisplaysSpeciesTree", DisplaysSpeciesTree.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("NumberTreeSpecies", NumberTreeSpecies.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("MaximunHeightTree", MaximunHeightTree.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("HighSorting", HighSorting.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("OldestDistrict", OldestDistrict.class,
                    "A map/reduce program that counts the words in the input files.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}

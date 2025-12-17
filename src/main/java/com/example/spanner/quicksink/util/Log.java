package com.example.spanner.quicksink.util;

public class Log {
    public static final int LEVEL_INFO = 1;
    public static final int LEVEL_DEBUG = 2;
    public static final int LEVEL_TRACE = 3;

    private static int currentLevel = LEVEL_INFO;

    public static void setLevel(int level) {
        currentLevel = level;
    }

    public static boolean isEnabled(int level) {
        return currentLevel >= level;
    }

    public static void info(String msg) {
        if (currentLevel >= LEVEL_INFO) {
            System.out.println(msg);
        }
    }
    
    public static void error(String msg) {
        System.err.println(msg);
    }

    public static void debug(String msg) {
        if (currentLevel >= LEVEL_DEBUG) {
            System.out.println(msg);
        }
    }

    public static void trace(String msg) {
        if (currentLevel >= LEVEL_TRACE) {
            System.out.println(msg);
        }
    }
}

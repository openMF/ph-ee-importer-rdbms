package hu.dpc.phee.operator;

public class OperatorUtils {

    public static String strip(String str) {
        return str.replaceAll("^\"|\"$", "");
    }
}

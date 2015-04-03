package ch.epfl.data.plan_runner.utilities;


import ddbt.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

public class DBToasterCompiler {

    private static Logger LOG = Logger.getLogger(DBToasterCompiler.class);

    static String DEFAULT_LOCATION = "/tmp/";

    /**
     * Write content to tmp file and return file path
     * @param content
     * @return
     */
    private static String writeTmpFile(String content) throws IOException {
        File tmp = File.createTempFile("squall", ".sql");
        FileOutputStream outputStream = new FileOutputStream(tmp);
        outputStream.write(content.getBytes("UTF-8"));
        outputStream.close();
        return tmp.getAbsolutePath();
    }

    private static String getQueryName() {
        return "Query" + (new Date().getTime() / 1000);
    }

    public static String compile(String sql) {
        return compile(sql, getQueryName());
    }

    public static String compile(String sql, String queryName) {
        return compile(sql, queryName, DEFAULT_LOCATION);
    }

    public static String compile(String sql, String queryName, String destLocation) {

        try {
            LOG.info("SQL content: \n" + sql);

            String tmpSQLFile = writeTmpFile(sql);
            LOG.info("Created tmp sql file: " + tmpSQLFile);

            String[] args = new String[]{
                    "-xd", destLocation,
                    "-l", "scala",
                    tmpSQLFile,
                    "-n", queryName,
                    "-o", queryName + ".scala",
                    "-c", queryName + ".jar"};

            LOG.info("Generate DBToaster Code with args: " + Arrays.toString(args));
            ddbt.Compiler.main(args);
            return destLocation + queryName + ".jar"; // return the location of generated jar file

        } catch (IOException e) {
            throw new RuntimeException("DBToaster: Unable to compile SQL", e);
        }

        //        String[] a = new String[] {"-xd","/Users/khuevu/Projects/DDBToaster/tmp", "-l", "scala",  "/Users/khuevu/Projects/dbtoaster/modifiedrst.sql","-o", "random.scala", "-c", "random.jar"};
//        Compiler.main(a);
//        Class testClass = this.getClass().getClassLoader().loadClass("ddbt.gen.Random");
//        System.out.println(testClass.getName());

    }
}

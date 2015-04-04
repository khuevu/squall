package ch.epfl.data.plan_runner.utilities;


import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static java.nio.file.StandardCopyOption.*;


public class JarUtilities {

    private static Logger LOG = Logger.getLogger(JarUtilities.class);


    /**
     * Extract jarFile to destDir
     *
     * @param jarFile
     * @param destDir
     * @throws IOException
     */
    public static void extractJarFile(String jarFile, String destDir) throws IOException {
        LOG.info("Extracting Jar file: " + jarFile + " to : " + destDir);
        java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile);
        java.util.Enumeration enumEntries = jar.entries();
        while (enumEntries.hasMoreElements()) {
            java.util.jar.JarEntry file = (java.util.jar.JarEntry) enumEntries.nextElement();
            java.io.File f = new java.io.File(destDir + java.io.File.separator + file.getName());

            if (!file.isDirectory()) {
                f.getParentFile().mkdirs();
                java.io.InputStream is = jar.getInputStream(file); // get the input stream
                Files.copy(is, f.toPath(), REPLACE_EXISTING);
                is.close();
            }
        }
    }

    /**
     * Create jar file from input directory
     *
     * @param targetJar
     * @param inputDirectory
     * @throws IOException
     */
    public static void createJar(String targetJar, String inputDirectory) throws IOException {
        LOG.info("Createing Jar " + targetJar + " from files in : " + inputDirectory);
        JarOutputStream target = new JarOutputStream(new FileOutputStream(targetJar));
        addToJar(new File(inputDirectory), target, inputDirectory);
        target.close();
    }

    private static void addToJar(File source, JarOutputStream target, String inputDirectory) throws IOException {

        if (source.isDirectory()) {
            for (File nestedFile : source.listFiles())
                addToJar(nestedFile, target, inputDirectory);

        } else {

            String relativeName = getRelativePath(inputDirectory, source.getPath());
            JarEntry entry = new JarEntry(relativeName);

            entry.setTime(source.lastModified());
            target.putNextEntry(entry);

            Files.copy(source.toPath(), target);
            target.closeEntry();
        }
    }


    private static String getRelativePath(String location, String file) {
        Path locationPath = Paths.get(location);
        Path filePath = Paths.get(file);
        Path relative = locationPath.relativize(filePath);
        return relative.toString();
    }
}

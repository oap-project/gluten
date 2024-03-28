/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.vectorized;

import org.apache.gluten.exception.GlutenException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class JniResourceHelper {
  private static final Logger LOG = LoggerFactory.getLogger(JniResourceHelper.class);

  private final String workDir;
  private final Set<String> jarExtracted = new HashSet<>();

  private boolean headersExtracted = false;

  JniResourceHelper(String workDir) {
    this.workDir = workDir;
  }

  private static void extractHeaders0(String workDir) throws IOException {
    if (workDir == null) {
      workDir = System.getProperty("java.io.tmpdir");
    }
    final String folderToLoad = "include";
    final URL connResource = JniResourceHelper.class.getClassLoader().getResource("include");
    if (connResource != null) {
      final URLConnection urlConnection = connResource.openConnection();
      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        extractResourcesToDirectory(jarFile, folderToLoad, workDir + "/" + "nativesql_include");
      } else {
        // For Maven test only
        String path = urlConnection.getURL().toString();
        if (urlConnection.getURL().toString().startsWith("file:")) {
          // remove the prefix of "file:" from includePath
          path = urlConnection.getURL().toString().substring(5);
        }
        final File folder = new File(path);
        copyResourcesToDirectory(urlConnection, workDir + "/" + "nativesql_include", folder);
      }
    } else {
      LOG.info("There is no include dir in jars.");
    }
  }

  private static void copyResourcesToDirectory(
      URLConnection urlConnection, String destPath, File folder) {
    for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
      String destFilePath = destPath + "/" + fileEntry.getName();
      File destFile = new File(destFilePath);
      if (fileEntry.isDirectory()) {
        destFile.mkdirs();
        copyResourcesToDirectory(urlConnection, destFilePath, fileEntry);
      } else {
        try {
          Files.copy(fileEntry.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          LOG.error("Error copying file", e);
        }
      }
    }
  }

  private static void extractJar(String sourceJar, String workDir) throws IOException {
    if (workDir == null) {
      workDir = System.getProperty("java.io.tmpdir");
    }
    final String folderToLoad = "";
    URL url = new URL("jar:file:" + sourceJar + "!/");
    final URLConnection urlConnection = url.openConnection();
    File workDirHandler = new File(workDir + "/tmp");
    if (!workDirHandler.exists()) {
      workDirHandler.mkdirs();
    }

    if (urlConnection instanceof JarURLConnection) {
      final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
      extractResourcesToDirectory(jarFile, folderToLoad, workDir + "/tmp/");
    } else {
      throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
    }
    /*
     * System.out.println("Current content under " + workDir + "/tmp/");
     * Files.list(new File(workDir + "/tmp/").toPath()).forEach(path -> {
     * System.out.println(path); });
     */
  }

  private static void extractResourcesToDirectory(JarFile origJar, String jarPath, String destPath)
      throws IOException {
    for (Enumeration<JarEntry> entries = origJar.entries(); entries.hasMoreElements(); ) {
      JarEntry oneEntry = entries.nextElement();
      if (((Objects.equals(jarPath, "") && !oneEntry.getName().contains("META-INF"))
              || (oneEntry.getName().startsWith(jarPath + "/")))
          && !oneEntry.isDirectory()) {
        int rmLength = jarPath.isEmpty() ? 0 : jarPath.length() + 1;
        if (Files.exists(Paths.get(destPath + "/" + oneEntry.getName().substring(rmLength)))) {
          continue;
        }
        File destFile = new File(destPath + "/" + oneEntry.getName().substring(rmLength));
        File parentFile = destFile.getParentFile();
        if (parentFile != null) {
          parentFile.mkdirs();
        }

        FileOutputStream outFile = new FileOutputStream(destFile);
        InputStream inFile = origJar.getInputStream(oneEntry);

        try {
          byte[] buffer = new byte[4 * 1024];

          int s = 0;
          while ((s = inFile.read(buffer)) > 0) {
            outFile.write(buffer, 0, s);
          }
        } catch (IOException e) {
          throw new IOException("Could not extract resource from jar", e);
        } finally {
          try {
            inFile.close();
          } catch (IOException e) {
            LOG.error("Error closing file", e);
          }
          try {
            outFile.close();
          } catch (IOException e) {
            LOG.error("Error closing file", e);
          }
        }
      }
    }
  }

  public synchronized void extractHeaders() {
    try {
      if (headersExtracted) {
        LOG.debug("Headers already extracted to work directory {}, skipping", workDir);
        return;
      }
      LOG.info("Trying to extract headers to work directory {}", workDir);
      extractHeaders0(workDir);
      LOG.info("Successfully extracted headers to work directory {}", workDir);
      headersExtracted = true;
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  public synchronized void extractJars(List<String> jars) {
    jars.stream()
        .filter(
            jar -> {
              if (jarExtracted.contains(jar)) {
                LOG.debug("Jar {} already extracted to work directory {}, skipping", jar, workDir);
                return false;
              }
              return true;
            })
        .forEach(
            jar -> {
              try {
                LOG.info("Trying to extract jar {} to work directory {}", jar, workDir);
                extractJar(jar, workDir);
                LOG.info("Successfully extracted jar {} to work directory {}", jar, workDir);
                jarExtracted.add(jar);
              } catch (IOException e) {
                throw new GlutenException(e);
              }
            });
  }
}

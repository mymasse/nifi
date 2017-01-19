package com.mymasse.processors.springboot;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SpringBootFactory {

  private static final Logger logger = LoggerFactory.getLogger(SpringBootFactory.class);

  private static final String SC_DELEGATE_NAME = "com.mymasse.processors.springboot.bootstrap.SpringBootDelegate";

  static SpringDataExchanger createSpringBootDelegate(String springBootClass, String springBootJarFile) {
    List<URL> urls = gatherClassPathUrls(springBootJarFile);
    SpringBootClassLoader contextCl = new SpringBootClassLoader(urls.toArray(new URL[] {}),
        SpringBootFactory.class.getClassLoader());
    ClassLoader tContextCl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(contextCl);
    try {
      InputStream delegateStream = contextCl.getResourceAsStream(SC_DELEGATE_NAME.replace('.', '/') + ".class");
      byte[] delegateBytes = IOUtils.toByteArray(delegateStream);
      Class<?> clazz = contextCl.doDefineClass(SC_DELEGATE_NAME, delegateBytes, 0, delegateBytes.length);
      Constructor<?> ctr = clazz.getDeclaredConstructor(String.class, ClassLoader.class);
      ctr.setAccessible(true);
      SpringDataExchanger springDelegate = (SpringDataExchanger) ctr.newInstance(springBootClass, contextCl);
      if (logger.isInfoEnabled()) {
        logger.info("Successfully instantiated Spring Boot Application from '" + springBootJarFile + "'");
      }
      return springDelegate;
    } catch (Exception e) {
      try {
        contextCl.close();
      } catch (Exception e2) {
        // ignore
      }
      throw new IllegalStateException("Failed to instantiate Spring Boot Application. Class: '" + springBootClass
          + "'; Classpath: " + Arrays.asList(urls), e);
    } finally {
      Thread.currentThread().setContextClassLoader(tContextCl);
    }
  }

  static List<URL> gatherClassPathUrls(String bootJarPath) {
    if (logger.isDebugEnabled()) {
      logger.debug("Adding '" + bootJarPath + "' to the classpath.");
    }
    File bootJarPathFile = new File(bootJarPath);
    if (bootJarPathFile.exists() && bootJarPathFile.isFile()
        && bootJarPathFile.getName().toLowerCase().endsWith(".jar")) {
      try {
        List<URL> urls = new ArrayList<>();

        URL url = bootJarPathFile.toURI().toURL();
        urls.add(url);

        return urls;
      } catch (Exception e) {
        throw new IllegalStateException("Failed to parse Jar from '" + bootJarPathFile.getAbsolutePath() + "'", e);
      }
    } else {
      throw new IllegalArgumentException("Path '" + bootJarPathFile.getAbsolutePath()
          + "' is not valid because it doesn't exist or does not point to a jar file.");
    }
  }

  private static class SpringBootClassLoader extends URLClassLoader {
    public SpringBootClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    public final Class<?> doDefineClass(String name, byte[] b, int off, int len) {
      return this.defineClass(name, b, off, len);
    }
  }

}

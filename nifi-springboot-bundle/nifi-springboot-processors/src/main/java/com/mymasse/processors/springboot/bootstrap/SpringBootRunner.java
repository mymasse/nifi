package com.mymasse.processors.springboot.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public class SpringBootRunner {

  private ConfigurableApplicationContext appContext;
  private final Class<?> className;

  private Object monitor = new Object();
  private boolean shouldWait;

  protected SpringBootRunner(final Class<?> className) {
    this.className = className;
  }

  public void run() {
    if (appContext != null) {
      throw new IllegalStateException("AppContext must be null to run this backend");
    }
    runBootInThread();
    waitUntilBootIsStarted();
  }

  public ConfigurableApplicationContext getAppContext() {
    return appContext;
  }

  private void waitUntilBootIsStarted() {
    try {
      synchronized (monitor) {
        if (shouldWait) {
          monitor.wait();
        }
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private void runBootInThread() {
    final Thread runnerThread = new SpringBootRunnerThread();
    shouldWait = true;
    runnerThread.setContextClassLoader(className.getClassLoader());
    runnerThread.start();
  }

  public void stop() {
    SpringApplication.exit(appContext);
    appContext = null;
  }

  private class SpringBootRunnerThread extends Thread {
    @Override
    public void run() {
      appContext = SpringApplication.run(className, new String[] {});
      synchronized (monitor) {
        shouldWait = false;
        monitor.notify();
      }
    }
  }
}

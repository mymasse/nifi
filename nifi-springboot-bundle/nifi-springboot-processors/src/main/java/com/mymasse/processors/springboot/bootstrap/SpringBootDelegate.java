package com.mymasse.processors.springboot.bootstrap;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URLClassLoader;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.MessageBuilder;

import com.mymasse.processors.springboot.SpringDataExchanger;
import com.mymasse.processors.springboot.SpringNiFiConstants;

/*
 * This class is for internal use only and must never be instantiated by the NAR
 * Class Loader (hence in a isolated package with nothing referencing it). It is
 * loaded by a dedicated CL via byte array that represents it ensuring that this
 * class can be loaded multiple times by multiple Class Loaders within a single
 * instance of NAR.
 */
public class SpringBootDelegate implements Closeable, SpringDataExchanger {

  private final Logger logger = LoggerFactory.getLogger(SpringBootDelegate.class);

  private final ConfigurableApplicationContext applicationContext;

  private final MessageChannel toSpringChannel;

  private final PollableChannel fromSpringChannel;

  private final String className;

  private final SpringBootRunner runnerInstance;

  private SpringBootDelegate(String className, ClassLoader classLoader) {
    this.className = className;
    ClassLoader orig = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    if (logger.isDebugEnabled()) {
      logger.debug("Using " + Thread.currentThread().getContextClassLoader()
          + " as context class loader while loading Spring Boot Application '" + className + "'.");
    }
    try {
      Class<?> runnerClass = this.getClass().getClassLoader()
          .loadClass("com.mymasse.processors.springboot.bootstrap.SpringBootRunner");
      Constructor<?> ctr = runnerClass.getDeclaredConstructor(Class.class);
      ctr.setAccessible(true);
      runnerInstance = (SpringBootRunner) ctr.newInstance(this.getClass().getClassLoader().loadClass(className));
      runnerClass.getMethod("run").invoke(runnerInstance);

      this.applicationContext = runnerInstance.getAppContext();

      if (this.applicationContext.containsBean(SpringNiFiConstants.FROM_NIFI)) {
        this.toSpringChannel = this.applicationContext.getBean(SpringNiFiConstants.FROM_NIFI, MessageChannel.class);
        if (logger.isDebugEnabled()) {
          logger.debug("Spring Boot Application '" + className
              + "' is capable of receiving messages from NiFi since 'fromNiFi' channel was discovered.");
        }
      } else {
        this.toSpringChannel = null;
      }
      if (this.applicationContext.containsBean(SpringNiFiConstants.TO_NIFI)) {
        this.fromSpringChannel = this.applicationContext.getBean(SpringNiFiConstants.TO_NIFI, PollableChannel.class);
        if (logger.isDebugEnabled()) {
          logger.debug("Spring Boot Application '" + className + "' is capable of sending messages to "
              + "NiFi since 'toNiFi' channel was discovered.");
        }
      } else {
        this.fromSpringChannel = null;
      }
      if (logger.isInfoEnabled() && this.toSpringChannel == null && this.fromSpringChannel == null) {
        logger.info("Spring Boot Application '" + className
            + "' is headless since neither 'fromNiFi' nor 'toNiFi' channels were defined. No data will be exchanged.");
      }
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(orig);
    }
  }

  @Override
  public <T> boolean send(T payload, Map<String, ?> headers, long timeout) {
    if (this.toSpringChannel != null) {
      return this.toSpringChannel.send(MessageBuilder.withPayload(payload).copyHeaders(headers).build(), timeout);
    } else {
      throw new IllegalStateException("Failed to send message to '" + this.className
          + "'. There are no 'fromNiFi' channels configured which means the Spring Boot Application is not set up to receive messages from NiFi");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SpringResponse<T> receive(long timeout) {
    if (this.fromSpringChannel != null) {
      final Message<T> message = (Message<T>) this.fromSpringChannel.receive(timeout);
      if (message != null) {
        if (!(message.getPayload() instanceof byte[]) && !(message.getPayload() instanceof String)) {
          throw new IllegalStateException("Failed while receiving message from Spring due to the "
              + "payload type being other then byte[] or String which are the only types that are supported. Please "
              + "apply transformation/conversion on Spring side when sending message back to NiFi");
        }
        return new SpringResponse<T>(message.getPayload(), message.getHeaders());
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    logger.info("Closing Spring Boot Application");
    runnerInstance.stop();
    if (logger.isInfoEnabled()) {
      logger.info("Closing " + this.getClass().getClassLoader());
    }
    ((URLClassLoader) this.getClass().getClassLoader()).close();
    logger.info("Successfully closed Spring Boot Application and its ClassLoader.");
  }

}

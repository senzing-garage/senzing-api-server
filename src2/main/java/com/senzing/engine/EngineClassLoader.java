package com.senzing.engine;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.HashMap;

public class EngineClassLoader extends ClassLoader {
  /**
   * Cache of classes for class name to class obejcts.
   */
  private Map<String, Class<?>> classCache;

  /**
   *  Default constructor.
   */
  public EngineClassLoader() {
    this.classCache = new HashMap<String, Class<?>>();
  }

  /**
   * Constructs with the parent class loader.
   */
  public EngineClassLoader(ClassLoader parent) {
    super(parent);
    this.classCache = new HashMap<String, Class<?>>();
  }

  /**
   * Implemented to find the specified class.
   */
  protected synchronized Class<?> findClass(String name) throws ClassNotFoundException {
    Class<?> c = this.classCache.get(name);
    if (c != null) return c;

    byte[] classData = null;
    try {
      classData = this.loadClassData(name);
    } catch (IOException e) {
      throw new ClassNotFoundException("Failed to find class: " + name, e);
    }

    c = this.defineClass(name, classData, 0, classData.length);
    this.resolveClass(c);
    this.classCache.put(name, c);
    return c;
  }

  /**
   * Implemented to load the class data for the specified class name.
   */
  protected byte[] loadClassData(String name) throws IOException {
    String resourcePath =  name.replace(".", "/") + ".class";

    ClassLoader parent = this.getParent();
    if (parent == null) parent = ClassLoader.getSystemClassLoader();

    System.out.println("RESOURCE PATH: " + resourcePath);

    InputStream is = parent.getResourceAsStream(resourcePath);
    if (is == null) {
      throw new IOException("Resource not found: " + resourcePath);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] buffer = new byte[8192];

    for (int readCount = is.read(buffer);
         readCount >= 0;
         readCount = is.read(buffer))
    {
      baos.write(buffer, 0, readCount);
    }

    try {
      is.close();
    } catch (IOException ignore) {
      // ignore the exception
    }

    return baos.toByteArray();
  }
}

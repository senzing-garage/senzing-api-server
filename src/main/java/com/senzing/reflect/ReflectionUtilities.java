package com.senzing.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

public class ReflectionUtilities {
  /**
   * Provides a synchronized handler that synchronizes on the proxy for
   * every method that is invoked.
   */
  private static class SynchronizedHandler implements InvocationHandler {
    /**
     * The target object for invoking the method.
     */
    private Object target;

    /**
     * The object to synchronize on.
     */
    private Object monitor = null;

    /**
     * Constructs with the specified target object on which to invoke the
     * methods.  The constructed instance will synchronize on the proxy object
     * that is passed to {@link #invoke(Object, Method, Object[]))}.
     *
     * @param target The target object to invoke the methods on.
     */
    public SynchronizedHandler(Object target) {
      this(target, null);
    }

    /**
     * Constructs with the specified target object on which to invoke the
     * methods and the specified object to synchronize on, or <tt>null</tt> if
     * the constructed instance should synchronize on the proxy object that is
     * passed to {@link #invoke(Object, Method, Object[]))}.
     *
     * @param target  The target object to invoke the methods on.
     * @param monitor The object to synchronize on, or <tt>null</tt> if the
     *                constructed instance should synchronize on the proxy
     *                object that is passed to {@link
     *                #invoke(Object, Method, Object[]))}.
     */
    public SynchronizedHandler(Object target, Object monitor) {
      this.target = target;
      this.monitor = monitor;
    }

    /**
     * Overridden to synchronize on the specified proxy object before calling
     * the specified {@link Method} on the underlying target object with the
     * specified arguments.
     *
     * @param proxy  The proxy object to synchronize on.
     * @param method The {@link Method} to invoke on the underlying target
     *               object.
     * @param args   The arguments to invoke the method with.
     * @return The result from invoking the method.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      final Object monitor = (this.monitor == null) ? proxy : this.monitor;
      synchronized (monitor) {
        try {
          return method.invoke(this.target, args);

        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Creates a synchronized proxy wrapper for the specified {@link Object}
   * using the specified proxy interface class.  The returned instance will
   * synchronize on the returned proxy object before invoking any method
   * on the specified target object.
   *
   * @param proxyInterface The interface that the returned synchronized proxy
   *                       will implement.
   * @param targetObject The target object to wrap with the proxy.
   *
   * @return The synchronized proxy wrapper.
   */
  public static <I,T extends I> I synchronizedProxy(Class<I>  proxyInterface,
                                                    T         targetObject)
  {
    return synchronizedProxy(proxyInterface, targetObject, null);
  }

  /**
   * Creates a synchronized proxy wrapper for the specified {@link Object}
   * using the specified proxy interface class and specified monitor object to
   * synchronize on.  If the specified monitor object is <tt>null</tt> then
   * the returned instance will synchronize on the returned proxy object before
   * invoking any method on the specified target object.
   *
   * @param proxyInterface The interface that the returned synchronized proxy
   *                       will implement.
   * @param targetObject The target object to wrap with the proxy.
   * @param monitor The monitor object to synchronize on.
   *
   * @return The synchronized proxy wrapper.
   */
  public static <I,T extends I> I synchronizedProxy(Class<I>  proxyInterface,
                                                    T         targetObject,
                                                    Object    monitor)
  {
    // check the parameters
    Objects.requireNonNull(
        proxyInterface, "The proxy interface cannot be null.");
    Objects.requireNonNull(
        targetObject, "The specified target object cannot be null.");
    if (!proxyInterface.isInterface()) {
      throw new IllegalArgumentException(
          "The specified proxy class is not an interface: "
              + proxyInterface.getName());
    }
    if (!proxyInterface.isAssignableFrom(targetObject.getClass())) {
      throw new IllegalArgumentException(
          "The specified target object does not implement the specified proxy "
          + "interface.  proxyInterface=[ " + proxyInterface.getName() + " ], "
          + "targetObjectClass=[ " + targetObject.getClass().getName() + " ]");
    }
    ClassLoader         classLoader = targetObject.getClass().getClassLoader();
    Class[]             interfaces  = { proxyInterface };
    SynchronizedHandler handler     = new SynchronizedHandler(targetObject,
                                                              monitor);

    return (I) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }
}

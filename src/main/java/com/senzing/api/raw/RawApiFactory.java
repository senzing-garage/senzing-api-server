package com.senzing.api.raw;

import com.senzing.g2.engine.*;
import com.senzing.util.AccessToken;

import java.util.Objects;

/**
 * Provides an abstraction for creating instances of the raw Senzing API.
 * This abstraction allows for alternate implementations to be used especially
 * during auto tests.
 *
 */
public class RawApiFactory {
  /**
   * The current {@link AccessToken} required to authorize uninstalling the
   * {@link RawApiProvider}, or <tt>null</tt> if no provider is installed.
   */
  private static AccessToken current_token = null;

  /**
   * The currently installed {@link RawApiProvider}, or <tt>null</tt> if no
   * provider is installed.
   */
  private static RawApiProvider api_provider = null;

  /**
   * Installs the {@link RawApiProvider} to be used by the factory.  If
   * none is installed then the default mechanism of constructing new
   * raw API objects is used.  This returns an {@link AccessToken} to be
   * used for uninstalling the provider later.
   *
   * @param provider The non-null {@link RawApiProvider} to install.
   *
   * @return The {@link AccessToken} to be used for uninstalling the provider.
   *
   * @throws NullPointerException If the specified provider is <tt>null</tt>.
   *
   * @throws IllegalStateException If a provider is already installed and must
   *                               first be uninstalled.
   */
  public synchronized static AccessToken installProvider(RawApiProvider provider) {
    Objects.requireNonNull(provider, "The specified provider cannot be null.");
    if (current_token != null) {
      throw new IllegalStateException(
          "A provider is already installed and must first be uninstalled.");
    }
    api_provider  = provider;
    current_token = new AccessToken();
    return current_token;
  }

  /**
   * Checks if a {@link RawApiProvider} has been installed.
   *
   * @return <tt>true</tt> if a {@link RawApiProvider} has been installed,
   *         otherwise <tt>false</tt>.
   */
  public synchronized static boolean isProviderInstalled() {
    return (current_token != null);
  }

  /**
   * Uninstalls a previously installed {@link RawApiProvider} using the
   * specified {@link AccessToken} to authorize the operation.  If no provider
   * has been installed then this method does nothing.
   *
   * @param token The {@link AccessToken} that was returned when installing the
   *              provider.
   *
   * @throws IllegalArgumentException If the specified token does not match the
   *                                  token that was returned when the provider
   *                                  was installed.
   */
  private synchronized static void uninstallProvider(AccessToken token) {
    if (current_token == null) {
      return;
    }
    if (!current_token.equals(token)) {
      throw new IllegalArgumentException(
          "The specified access token is not the expected access token to "
          + "authorize unintalling the provider.");
    }
    current_token = null;
    api_provider  = null;
  }

  /***
   * Internal method for getting the currently installed provider in a
   * thread-safe manner.
   *
   * @return The currently installed provider, or <tt>null</tt> if no provider
   *         is currently installed.
   */
  private synchronized static RawApiProvider getInstalledProvider() {
    return api_provider;
  }

  /**
   * Creates a new instance of {@link G2Engine} to use.  If a
   * {@link RawApiProvider} is installed then it is used to create
   * the instance, otherwise a new instance of {@link G2JNI} is
   * constructed and returned.
   *
   * @return A new instance of {@link G2Engine} to use.
   */
  public static G2Engine createEngineApi() {
    RawApiProvider provider = getInstalledProvider();
    if (provider != null) {
      return provider.createEngineApi();
    } else {
      return new G2JNI();
    }
  }

  /**
   * Provides a new instance of {@link G2Config} to use.  If a
   * {@link RawApiProvider} is installed then it is used to create
   * the instance, otherwise a new instance of {@link G2ConfigJNI} is
   * constructed and returned.
   *
   * @return A new instance of {@link G2Config} to use.
   */
  public static G2Config createConfigApi() {
    RawApiProvider provider = getInstalledProvider();
    if (provider != null) {
      return provider.createConfigApi();
    } else {
      return new G2ConfigJNI();
    }
  }

  /**
   * Provides a new instance of {@link G2Product} to use.  If a
   * {@link RawApiProvider} is installed then it is used to create
   * the instance, otherwise a new instance of {@link G2ProductJNI} is
   * constructed and returned.
   *
   * @return A new instance of {@link G2Product} to use.
   */
  public static G2Product createProductApi() {
    RawApiProvider provider = getInstalledProvider();
    if (provider != null) {
      return provider.createProductApi();
    } else {
      return new G2ProductJNI();
    }
  }

  /**
   * Provides a new instance of {@link G2ConfigMgr} to use.  If a
   * {@link RawApiProvider} is installed then it is used to create
   * the instance, otherwise a new instance of {@link G2ConfigMgrJNI} is
   * constructed and returned.
   *
   * @return A new instance of {@link G2ConfigMgr} to use.
   *
   */
  public static G2ConfigMgr createConfigMgrApi() {
    RawApiProvider provider = getInstalledProvider();
    if (provider != null) {
      return provider.createConfigMgrApi();
    } else {
      return new G2ConfigMgrJNI();
    }
  }

}

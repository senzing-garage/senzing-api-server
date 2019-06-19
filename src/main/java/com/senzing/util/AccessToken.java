package com.senzing.util;

import java.lang.ThreadLocal;

public final class AccessToken {
  public AccessToken() {}

  public boolean equals(Object obj) {
    return (this == obj);
  }

  public int hashCode() {
    return System.identityHashCode(this);
  }

  private static ThreadLocal<AccessToken> THREAD_ACCESS_TOKEN
    = new ThreadLocal<AccessToken>();

  /**
   * Returns the current thread-local {@link AccessToken} to use for granting
   * privileged access to restricted methods from objects.  The return value
   * from this method will be the same <b>until</b> the {@link AccessToken}
   * is claimed by {@link #claimThreadAccessToken()}.
   *
   * @return The current thread-local {@link AccessToken} for this thread which
   *         will change once {@link #claimThreadAccessToken()} is called.
   */
  public static AccessToken getThreadAccessToken() {
    AccessToken token = THREAD_ACCESS_TOKEN.get();
    if (token == null) {
      token = new AccessToken();
      THREAD_ACCESS_TOKEN.set(token);
    }
    return token;
  }

  /**
   * Claims the {@linkplain #getThreadAccessToken() current thread-local
   * access token} and forces it to be changed so that subsequent calls to
   * {@link #getThreadAccessToken()} will not return the same value.
   *
   * @return The previous thread-local {@link AccessToken} after changing the
   *         current thread-local {@link AccessToken}.
   */
  public static AccessToken claimThreadAccessToken() {
    AccessToken token = AccessToken.getThreadAccessToken();
    THREAD_ACCESS_TOKEN.set(new AccessToken());
    return token;
  }
}

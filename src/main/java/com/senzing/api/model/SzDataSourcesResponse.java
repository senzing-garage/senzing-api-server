package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * The response containing a set of data source codes.  Typically this is the
 * list of all configured data source codes.
 *
 */
public class SzDataSourcesResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzDataSourcesResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * data sources to be added later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzDataSourcesResponse(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink,
                               Timers       timers) {
    super(httpMethod, httpStatusCode, selfLink, timers);
    this.data.dataSources = new LinkedHashSet<>();
  }

  /**
   * Constructs with only the HTTP method and the {@link UriInfo}, leaving the
   * data sources to be added later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param timers The {@link Timers} object for the timings that were taken.
   *
   */
  public SzDataSourcesResponse(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               UriInfo      uriInfo,
                               Timers       timers)
  {
    super(httpMethod, httpStatusCode, uriInfo, timers);
    this.data.dataSources = new LinkedHashSet<>();
  }

  /**
   * Returns the {@link Data} for this instance.
   *
   * @return The {@link Data} for this instance.
   */
  public Data getData() {
    return this.data;
  }

  /**
   * Adds the specified data source providing the specified data source
   * is not already containe
   *
   * @param dataSource The data source code to add.
   */
  public void addDataSource(String dataSource) {
    dataSource = dataSource.trim().toUpperCase();
    if (this.data.dataSources.contains(dataSource)) return;
    this.data.dataSources.add(dataSource);
  }

  /**
   * Sets the specified {@link Set} of data sources using the
   * specified {@link Collection} of data sources (removing duplicates).
   *
   * @param dataSources The {@link Collection} of data sources to set.
   */
  public void setDataSources(Collection<String> dataSources)
  {
    this.data.dataSources.clear();
    if (dataSources != null) {
      this.data.dataSources.addAll(dataSources);
    }
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The set of data source codes.
     */
    private Set<String> dataSources;

    /**
     * Private default constructor.
     */
    private Data() {
      this.dataSources = null;
    }

    /**
     * Gets the unmodifiable {@link Set} of data sources.
     *
     * @return The unmodifiable {@link Set} of data sources.
     */
    public Set<String> getDataSources() {
      Set<String> set = this.dataSources;
      return Collections.unmodifiableSet(set);
    }
  }
}

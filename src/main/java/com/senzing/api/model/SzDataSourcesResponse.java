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
  public void addDataSource(SzDataSource dataSource) {
    this.data.dataSources.remove(dataSource.getDataSourceCode());
  }

  /**
   * Sets the specified {@link Set} of data sources using the
   * specified {@link Collection} of data sources (removing duplicates).
   *
   * @param dataSources The {@link Collection} of data sources to set.
   */
  public void setDataSources(Collection<SzDataSource> dataSources)
  {
    this.data.dataSources.clear();
    if (dataSources != null) {
      // ensure the data sources are unique
      for (SzDataSource dataSource : dataSources) {
        this.data.dataSources.put(dataSource.getDataSourceCode(), dataSource);
      }
    }
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The map of {@link String} data source codes to {@link SzDataSource}
     * instances.
     */
    private Map<String, SzDataSource> dataSources;

    /**
     * Private default constructor.
     */
    private Data() {
      this.dataSources = new LinkedHashMap<>();
    }

    /**
     * Gets the unmodifiable {@link Set} of data source codes.
     *
     * @return The unmodifiable {@link Set} of data source codes.
     */
    public Set<String> getDataSources() {
      Set<String> set = this.dataSources.keySet();
      return Collections.unmodifiableSet(set);
    }

    /**
     * Private setter used for deserialization.
     */
    private void setDataSources(Collection<String> dataSources) {
      Iterator<Map.Entry<String,SzDataSource>> iter
          = this.dataSources.entrySet().iterator();

      // remove entries in the map that are not in the specified set
      while (iter.hasNext()) {
        Map.Entry<String,SzDataSource> entry = iter.next();
        if (!dataSources.contains(entry.getKey())) {
          iter.remove();
        }
      }

      // add place-holder entries to the map for data sources in the set
      for (String dataSource: dataSources) {
        this.dataSources.put(dataSource, null);
      }
    }

    /**
     * Gets the unmodifiable {@link Map} of {@link String} data source codes
     * to {@link SzDataSource} values describing the configured data sources.
     *
     * @return The unmodifiable {@link Map} of {@link String} data source codes
     *         to {@link SzDataSource} values describing the configured data
     *         sources.
     */
    public Map<String, SzDataSource> getDataSourceDetails() {
      return Collections.unmodifiableMap(this.dataSources);
    }

    /**
     * Private setter used for deserialization.
     */
    private void setDataSourceDetails(Map<String, SzDataSource> details) {
      this.dataSources.clear();
      for (SzDataSource dataSource: details.values()) {
        this.dataSources.put(dataSource.getDataSourceCode(), dataSource);
      }
    }
  }
}

package com.senzing.api.model;

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
   * The set of data source codes.
   */
  private Set<String> dataSources;

  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * data sources to be added later.
   *
   * @param httpMethod The {@link SzHttpMethod}.
   *
   * @param httpStatusCode The HTTP response status code.
   *
   * @param selfLink The string URL link to generate this response.
   */
  public SzDataSourcesResponse(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               String       selfLink) {
    super(httpMethod, httpStatusCode, selfLink);
    this.dataSources = new LinkedHashSet<>();
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
   */
  public SzDataSourcesResponse(SzHttpMethod httpMethod,
                               int          httpStatusCode,
                               UriInfo      uriInfo) {
    super(httpMethod, httpStatusCode, uriInfo);
    this.dataSources = new LinkedHashSet<>();
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
    if (this.dataSources.contains(dataSource)) return;
    this.dataSources.add(dataSource);
  }

  /**
   * Sets the specified {@link Set} of data sources using the
   * specified {@link Collection} of data sources (removing duplicates).
   *
   * @param dataSources The {@link Collection} of data sources to set.
   */
  public void setDataSources(Collection<String> dataSources)
  {
    this.dataSources.clear();
    if (dataSources != null) {
      this.dataSources.addAll(dataSources);
    }
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public class Data {
    /**
     * Private default constructor.
     */
    private Data() {
      // do nothing
    }

    /**
     * Gets the unmodifiable {@link Set} of data sources.
     *
     * @return The unmodifiable {@link Set} of data sources.
     */
    public Set<String> getDataSources() {
      Set<String> set = SzDataSourcesResponse.this.dataSources;
      return Collections.unmodifiableSet(set);
    }
  }
}

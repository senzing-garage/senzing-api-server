package com.senzing.api.model;

import com.senzing.util.Timers;

import javax.ws.rs.core.UriInfo;
import java.util.*;

/**
 * The response containing a set of data source codes.  Typically this is the
 * list of all configured data source codes.
 *
 */
public class SzDataSourceResponse extends SzResponseWithRawData
{
  /**
   * The data for this instance.
   */
  private Data data = new Data();

  /**
   * Package-private default constructor.
   */
  SzDataSourceResponse() {
    // do nothing
  }

  /**
   * Constructs with only the HTTP method and the self link, leaving the
   * data sources to be added later.
   *
   * @param meta The response meta data.
   *
   * @param links The links for the response.
   *
   */
  public SzDataSourceResponse(SzMeta meta, SzLinks links) {
    super(meta, links);
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
   * Private setter for JSON marshalling.
   */
  private void setData(Data data) {
    this.data = data;
  }

  /**
   * Adds the specified data source providing the specified data source
   * is not already containe
   *
   * @param dataSource The data source code to add.
   */
  public void setDataSource(SzDataSource dataSource) {
    this.data.dataSource = dataSource;
  }

  /**
   * Inner class to represent the data section for this response.
   */
  public static class Data {
    /**
     * The {@link SzDataSource} for this instance.
     */
    private SzDataSource dataSource;

    /**
     * Private default constructor.
     */
    private Data() {
      this.dataSource = null;
    }

    /**
     * Gets the {@link SzDataSource} describing the data source.
     *
     * @return The {@link SzDataSource} describing the data source.
     */
    public SzDataSource getDataSource() {
      return this.dataSource;
    }

    /**
     * Private setter used for deserialization.
     */
    private void setDataSource(SzDataSource dataSource) {
      this.dataSource = dataSource;
    }
  }
}

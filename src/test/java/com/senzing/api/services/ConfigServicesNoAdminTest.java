package com.senzing.api.services;

import com.senzing.api.model.SzDataSource;
import com.senzing.api.model.SzEntityClass;
import com.senzing.api.model.SzEntityType;
import com.senzing.api.model.SzErrorResponse;
import com.senzing.api.server.SzApiServer;
import com.senzing.api.server.SzApiServerOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.core.UriInfo;
import java.io.UnsupportedEncodingException;
import java.util.Collections;

import static com.senzing.api.model.SzHttpMethod.POST;
import static com.senzing.api.services.ResponseValidators.validateBasics;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class ConfigServicesNoAdminTest extends ConfigServicesReadOnlyTest
{
  /**
   * Sets the desired options for the {@link SzApiServer} during server
   * initialization.
   *
   * @param options The {@link SzApiServerOptions} to initialize.
   */
  protected void initializeServerOptions(SzApiServerOptions options) {
    super.initializeServerOptions(options);
    options.setAdminEnabled(false);
    options.setReadOnly(false);
  }
}

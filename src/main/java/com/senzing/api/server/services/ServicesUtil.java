package com.senzing.api.server.services;

import com.senzing.api.model.*;
import com.senzing.g2.engine.G2Fallible;
import com.senzing.util.JsonUtils;

import javax.json.*;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

/**
 * Utility functions for services.
 */
public class ServicesUtil {
  /**
   * Creates an {@link InternalServerErrorException} and builds a response
   * with an {@link SzErrorResponse} using the specified {@link UriInfo}
   * and the specified exception.
   *
   * @param httpMethod The HTTP method for the request.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param exception The exception that caused the error.
   *
   * @return The {@link InternalServerErrorException}
   */
  static InternalServerErrorException newInternalServerErrorException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      Exception     exception)
  {
    Response.ResponseBuilder builder = Response.status(500);
    builder.entity(
        new SzErrorResponse(httpMethod, 500, uriInfo, exception));
    return new InternalServerErrorException(builder.build());
  }

  /**
   * Creates an {@link InternalServerErrorException} and builds a response
   * with an {@link SzErrorResponse} using the specified  {@link UriInfo}
   * and the specified {@link G2Fallible} to get the last exception from.
   *
   * @param httpMethod The HTTP method for the request.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param fallible The {@link G2Fallible} to get the last exception from.
   *
   * @return The {@link InternalServerErrorException}
   */
  static InternalServerErrorException newInternalServerErrorException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Fallible    fallible)
  {
    Response.ResponseBuilder builder = Response.status(500);
    builder.entity(
        new SzErrorResponse(httpMethod, 500, uriInfo, fallible));
    fallible.clearLastException();
    return new InternalServerErrorException(builder.build());
  }

  /**
   * Creates an {@link NotFoundException} and builds a response
   * with an {@link SzErrorResponse} using the specified {@link UriInfo}
   * and the specified {@link G2Fallible} to get the last exception from.
   *
   * @param httpMethod The HTTP method for the request.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param fallible The {@link G2Fallible} to get the last exception from.
   *
   * @return The {@link InternalServerErrorException}
   */
  static NotFoundException newNotFoundException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Fallible    fallible)
  {
    Response.ResponseBuilder builder = Response.status(404);
    builder.entity(
        new SzErrorResponse(httpMethod, 404, uriInfo, fallible));
    fallible.clearLastException();
    return new NotFoundException(builder.build());
  }

  /**
   * Creates an {@link NotFoundException} and builds a response
   * with an {@link SzErrorResponse} using the specified {@link UriInfo}.
   *
   * @param httpMethod The HTTP method for the request.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @return The {@link InternalServerErrorException}
   */
  static NotFoundException newNotFoundException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo)
  {
    Response.ResponseBuilder builder = Response.status(404);
    builder.entity(
        new SzErrorResponse(httpMethod, 404, uriInfo));
    return new NotFoundException(builder.build());
  }

  /**
   * Creates an {@link NotFoundException} and builds a response
   * with an {@link SzErrorResponse} using the specified {@link UriInfo}.
   *
   * @param httpMethod The HTTP method for the request.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param errorMessage The error message.
   *
   * @return The {@link InternalServerErrorException}
   */
  static NotFoundException newNotFoundException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      String        errorMessage)
  {
    Response.ResponseBuilder builder = Response.status(404);
    builder.entity(
        new SzErrorResponse(
            httpMethod, 404, uriInfo, errorMessage));
    return new NotFoundException(builder.build());
  }

  /**
   * Creates an {@link BadRequestException} and builds a response
   * with an {@link SzErrorResponse} using the specified {@link UriInfo}
   * and the specified {@link G2Fallible} to get the last exception from.
   *
   * @param httpMethod The HTTP method for the request.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param fallible The {@link G2Fallible} to get the last exception from.
   *
   * @return The {@link BadRequestException}
   */
  static BadRequestException newBadRequestException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      G2Fallible    fallible)
  {
    Response.ResponseBuilder builder = Response.status(400);
    builder.entity(
        new SzErrorResponse(httpMethod, 400, uriInfo, fallible));
    fallible.clearLastException();
    return new BadRequestException(builder.build());
  }

  /**
   * Creates an {@link BadRequestException} and builds a response
   * with an {@link SzErrorResponse} using the specified {@link UriInfo}.
   *
   * @param httpMethod The HTTP method for the request.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @param errorMessage The error message.
   *
   * @return The {@link BadRequestException} that was created with the
   *         specified http method and {@link UriInfo}.
   */
  static BadRequestException newBadRequestException(
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo,
      String        errorMessage)
  {
    Response.ResponseBuilder builder = Response.status(400);
    builder.entity(
        new SzErrorResponse(
            httpMethod, 400, uriInfo, errorMessage));
    return new BadRequestException(builder.build());
  }

  /**
   * URL encodes the specified text using UTF-8 encoding.
   *
   * @param text The text to encode.
   *
   * @return The URL-encoded text.
   */
  static String urlEncode(String text) {
    try {
      return URLEncoder.encode(text, "UTF-8");

    } catch (UnsupportedEncodingException cannotHappen) {
      throw new RuntimeException(cannotHappen);
    }
  }

  /**
   * Encodes a {@Link List} of {@link String} instances as a JSON array
   * string (without the leading and terminating brackets) with each of
   * the individual items being URL encoded.
   *
   * @param list The {@link List} of strings to encode.
   *
   * @return The encoded string.
   */
  static String urlEncodeStrings(List<String> list)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    for (String item : list) {
      sb.append(prefix);
      prefix = ",";
      try {
        sb.append(URLEncoder.encode(item, "UTF-8"));
      } catch (UnsupportedEncodingException cannotHappen) {
        // do nothing
      }
    }
    return sb.toString();
  }

  /**
   * Encodes a {@Link List} of {@link String} instances as a JSON array
   * string.
   *
   * @param list The {@link List} of strings to encode.
   *
   * @return The encoded string.
   */
  static String jsonEncodeStrings(List<String> list)
  {
    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (String item : list) {
      jab.add(item);
    }
    return JsonUtils.toJsonText(jab);
  }

  /**
   * Encodes a {@link List} of {@link SzEntityIdentifier} instances as a
   * JSON array string.
   *
   * @param list The list of entity identifiers.
   *
   * @return The JSON array string for the identifiers.
   */
  static String jsonEncodeEntityIds(List<SzEntityIdentifier> list)
  {
    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (SzEntityIdentifier id : list) {
      if (id instanceof SzEntityId) {
        jab.add(((SzEntityId) id).getValue());
      } else {
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("src", ((SzRecordId) id).getDataSourceCode());
        job.add("id", ((SzRecordId) id).getRecordId());
        jab.add(job);
      }
    }
    return JsonUtils.toJsonText(jab);
  }

  /**
   * Encodes a {@link Collection} of {@link SzEntityIdentifier} instances as a
   * JSON array string in the native Senzing format format for entity
   * identifiers.
   *
   * @param ids The {@link Collection} of entity identifiers.
   *
   * @return The JSON array string for the identifiers.
   */
  static String nativeJsonEncodeEntityIds(Collection<SzEntityIdentifier> ids) {
    if (ids == null) {
      return null;
    }
    String propName = "ENTITIES";
    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (SzEntityIdentifier id : ids) {
      if (id instanceof SzEntityId) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("ENTITY_ID", ((SzEntityId) id).getValue());
        jab.add(job);

      } else {
        propName = "RECORDS";
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("DATA_SOURCE", ((SzRecordId) id).getDataSourceCode());
        job.add("RECORD_ID", ((SzRecordId) id).getRecordId());
        jab.add(job);
      }
    }
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add(propName, jab);
    return JsonUtils.toJsonText(builder);
  }

  /**
   * Encodes a {@link Collection} of {@link SzEntityIdentifier} instances as a
   * JSON array string (without the leading and terminating brackets)
   * with each of the individual items in the list being URL encoded.
   *
   * @param ids The {@link Collection} of entity identifiers.
   *
   * @return The URL-encoded JSON array string for the identifiers.
   */
  static String urlEncodeEntityIds(Collection<SzEntityIdentifier> ids)
  {
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    for (SzEntityIdentifier id : ids) {
      sb.append(prefix);
      prefix = ",";
      if (id instanceof SzEntityId) {
        sb.append(((SzEntityId) id).getValue());
      } else {
        try {
          sb.append(URLEncoder.encode(id.toString(), "UTF-8"));
        } catch (UnsupportedEncodingException cannotHappen) {
          // do nothing
        }
      }
    }
    return sb.toString();
  }

  /**
   * Encodes a {@link List} of {@link String} data source codes in the
   * Senzing native JSON format for specifying data sources.
   *
   * @param list The list of entity identifiers.
   *
   * @return The JSON array string for the identifiers.
   */
  static String nativeJsonEncodeDataSources(List<String> list)
  {
    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (String code: list) {
      jab.add(code);
    }
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add("DATA_SOURCES", jab);
    return JsonUtils.toJsonText(jab);
  }

  /**
   * Formats multiple parameter values into a URL-encoded string of
   * multi-valued URL parameters.
   *
   * @param prefix The prefix to use (either "&" or "?").
   *
   * @param paramName The parameter name.
   *
   * @param values The {@link List} of values.
   *
   * @return The multi-valued query string parameter.
   */
  static String formatMultiValuedParam(String       prefix,
                                       String       paramName,
                                       List<String> values)
  {
    if (values == null || values.size() == 0) return "";
    StringBuilder sb = new StringBuilder();
    for (String val : values) {
      sb.append(prefix);
      sb.append(paramName);
      sb.append("=");
      sb.append(urlEncode(val));
      prefix = "&";
    }
    return sb.toString();
  }

  /**
   * Combines multiple {@link String} entity identifier parameters into a
   * JSON array and parses it as JSON to produce a {@link Set} of {@link
   * SzEntityIdentifier} instances.
   *
   * @param params The paramter values to parse.
   *
   * @param paramName The name of the parameter.
   *
   * @param httpMethod The HTTP method.
   *
   * @param uriInfo The {@link UriInfo} from the request.
   *
   * @return The {@link Set} of {@link SzEntityIdentifier} instances that was
   *         parsed from the specified parameters.
   */
  static Set<SzEntityIdentifier> parseEntityIdentifiers(
      List<String>  params,
      String        paramName,
      SzHttpMethod  httpMethod,
      UriInfo       uriInfo)
  {
    Set<SzEntityIdentifier> result = new LinkedHashSet<>();

    // check if the params is null or missing
    if (params == null || params.size() == 0) {
      return result;
    }

    // iterate over the params
    for (String param : params) {
      try {
        SzEntityIdentifier id = SzEntityIdentifier.valueOf(param);
        result.add(id);

      } catch (Exception e) {
        throw newBadRequestException(
            httpMethod, uriInfo,
            "Improperly formatted entity identifier parameter: "
            + paramName + "=" + param);
      }
    }

    return result;
  }
}

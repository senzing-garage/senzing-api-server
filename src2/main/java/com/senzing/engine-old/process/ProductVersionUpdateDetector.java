package com.senzing.api.engine.process;

import com.senzing.api.ProductVersion;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.senzing.api.Workbench.log;

public class ProductVersionUpdateDetector {

  boolean hasOutdatedProductVersions(long projectId) {
    try {
      ProductVersionDataAccess access = new ProductVersionDataAccess();
      List<ProductVersion> baseVersions;
      try (Connector baseConn = Connector.baseEntityRepo()) {
        baseVersions = access.getProductVersions(baseConn);
      }
      List<ProductVersion> projectVersions;
      try (Connector projectConn = Connector.entityRepo(projectId)) {
        projectVersions = access.getProductVersions(projectConn);
      }
      log("CHECKING FOR PRODUCT VERSIONS");
      return !intersectionEquals(baseVersions, projectVersions);
    }
    catch(Exception e) {
      log(e);
      return false;
    }
  }

  boolean intersectionEquals(List<ProductVersion> baseVersions, List<ProductVersion> projectVersions) {
    Map<String, String> baseVersionMap = asMap(baseVersions);
    Map<String, String> projectVersionMap = asMap(projectVersions);
    Set<String> commonProductNames = baseVersionMap.keySet();
    commonProductNames.retainAll(projectVersionMap.keySet());

    for (String commonProductName : commonProductNames) {
      if (!Objects.equals(baseVersionMap.get(commonProductName),
          projectVersionMap.get(commonProductName))) {
        return false;
      }
    }
    return true;
  }

  private Map<String, String> asMap(List<ProductVersion> baseVersions) {
    return baseVersions.stream()
        .collect(Collectors.toMap(ProductVersion::getProduct, ProductVersion::getVersion));
  }
}

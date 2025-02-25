/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.snowflake;

import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airbyte.commons.json.Jsons;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeDataSourceUtils {

  public static final String OAUTH_METHOD = "OAuth";
  public static final String USERNAME_PASSWORD_METHOD = "username/password";
  public static final String UNRECOGNIZED = "Unrecognized";

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeDataSourceUtils.class);
  private static final int PAUSE_BETWEEN_TOKEN_REFRESH_MIN = 7; // snowflake access token's TTL is 10min and can't be modified
  private static final String REFRESH_TOKEN_URL = "https://%s/oauth/token-request";
  private static final HttpClient httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_2)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  /**
   * Snowflake OAuth access token expires in 10 minutes. For the cases when sync duration is more than
   * 10 min, it requires updating 'token' property after the start of connection pool.
   * HikariDataSource brings support for this requirement.
   *
   * @param config source config JSON
   * @return datasource
   */
  public static HikariDataSource createDataSource(final JsonNode config) {
    HikariDataSource dataSource = new HikariDataSource();
    dataSource.setJdbcUrl(buildJDBCUrl(config));

    if (config.has("credentials")) {
      JsonNode credentials = config.get("credentials");
      final String authType = credentials.has("auth_type") ? credentials.get("auth_type").asText() : UNRECOGNIZED;
      switch (authType) {
        case OAUTH_METHOD -> {
          LOGGER.info("Authorization mode is OAuth");
          dataSource.setDataSourceProperties(buildAuthProperties(config));
          // thread to keep the refresh token up to date
          SnowflakeSource.SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(
              getAccessTokenTask(dataSource),
              PAUSE_BETWEEN_TOKEN_REFRESH_MIN, PAUSE_BETWEEN_TOKEN_REFRESH_MIN, TimeUnit.MINUTES);
        }
        case USERNAME_PASSWORD_METHOD -> {
          LOGGER.info("Authorization mode is 'Username and password'");
          populateUsernamePasswordConfig(dataSource, config.get("credentials"));
        }
        default -> throw new IllegalArgumentException("Unrecognized auth type: " + authType);
      }
    } else {
      LOGGER.info("Authorization mode is deprecated 'Username and password'. Please update your source configuration");
      populateUsernamePasswordConfig(dataSource, config);
    }

    return dataSource;
  }

  /**
   * Method to make request for a new access token using refresh token and client credentials.
   *
   * @return access token
   */
  public static String getAccessTokenUsingRefreshToken(final String hostName,
                                                       final String clientId,
                                                       final String clientSecret,
                                                       final String refreshToken)
      throws IOException {
    final var refreshTokenUri = String.format(REFRESH_TOKEN_URL, hostName);
    final Map<String, String> requestBody = new HashMap<>();
    requestBody.put("grant_type", "refresh_token");
    requestBody.put("refresh_token", refreshToken);

    try {
      final BodyPublisher bodyPublisher = BodyPublishers.ofString(requestBody.keySet().stream()
          .map(key -> key + "=" + URLEncoder.encode(requestBody.get(key), StandardCharsets.UTF_8))
          .collect(joining("&")));

      final byte[] authorization = Base64.getEncoder()
          .encode((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

      final HttpRequest request = HttpRequest.newBuilder()
          .POST(bodyPublisher)
          .uri(URI.create(refreshTokenUri))
          .header("Content-Type", "application/x-www-form-urlencoded")
          .header("Accept", "application/json")
          .header("Authorization", "Basic " + new String(authorization, StandardCharsets.UTF_8))
          .build();

      final HttpResponse<String> response = httpClient.send(request,
          HttpResponse.BodyHandlers.ofString());

      final JsonNode jsonResponse = Jsons.deserialize(response.body());
      if (jsonResponse.has("access_token")) {
        return jsonResponse.get("access_token").asText();
      } else {
        LOGGER.error("Failed to obtain accessToken using refresh token. " + jsonResponse);
        throw new RuntimeException(
            "Failed to obtain accessToken using refresh token.");
      }
    } catch (final InterruptedException e) {
      throw new IOException("Failed to refreshToken", e);
    }
  }

  public static String buildJDBCUrl(JsonNode config) {
    final StringBuilder jdbcUrl = new StringBuilder(String.format("jdbc:snowflake://%s/?",
        config.get("host").asText()));

    // Add required properties
    jdbcUrl.append(String.format(
        "role=%s&warehouse=%s&database=%s&schema=%s&JDBC_QUERY_RESULT_FORMAT=%s&CLIENT_SESSION_KEEP_ALIVE=%s",
        config.get("role").asText(),
        config.get("warehouse").asText(),
        config.get("database").asText(),
        config.get("schema").asText(),
        // Needed for JDK17 - see
        // https://stackoverflow.com/questions/67409650/snowflake-jdbc-driver-internal-error-fail-to-retrieve-row-count-for-first-arrow
        "JSON",
        true));

    // https://docs.snowflake.com/en/user-guide/jdbc-configure.html#jdbc-driver-connection-string
    if (config.has("jdbc_url_params")) {
      jdbcUrl.append("&").append(config.get("jdbc_url_params").asText());
    }
    return jdbcUrl.toString();
  }

  private static Runnable getAccessTokenTask(final HikariDataSource dataSource) {
    return () -> {
      LOGGER.info("Refresh token process started");
      var props = dataSource.getDataSourceProperties();
      try {
        var token = getAccessTokenUsingRefreshToken(props.getProperty("host"),
            props.getProperty("client_id"), props.getProperty("client_secret"),
            props.getProperty("refresh_token"));
        props.setProperty("token", token);
        dataSource.setDataSourceProperties(props);
        LOGGER.info("New access token has been obtained");
      } catch (IOException e) {
        LOGGER.error("Failed to obtain a fresh accessToken:" + e);
      }
    };
  }

  public static Properties buildAuthProperties(JsonNode config) {
    Properties properties = new Properties();
    try {
      var credentials = config.get("credentials");
      properties.setProperty("client_id", credentials.get("client_id").asText());
      properties.setProperty("client_secret", credentials.get("client_secret").asText());
      properties.setProperty("refresh_token", credentials.get("refresh_token").asText());
      properties.setProperty("host", config.get("host").asText());
      properties.put("authenticator", "oauth");
      properties.put("account", config.get("host").asText());

      String accessToken = getAccessTokenUsingRefreshToken(
          config.get("host").asText(), credentials.get("client_id").asText(),
          credentials.get("client_secret").asText(), credentials.get("refresh_token").asText());

      properties.put("token", accessToken);
    } catch (IOException e) {
      LOGGER.error("Request access token was failed with error" + e.getMessage());
    }
    return properties;
  }

  private static void populateUsernamePasswordConfig(HikariConfig hikariConfig, JsonNode config) {
    hikariConfig.setUsername(config.get("username").asText());
    hikariConfig.setPassword(config.get("password").asText());
  }

}

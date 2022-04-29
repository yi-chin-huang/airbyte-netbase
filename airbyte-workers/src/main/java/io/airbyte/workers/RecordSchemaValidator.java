/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Validates that AirbyteRecordMessage data conforms to the JSON schema defined by the source's
 * configured catalog
 */

public class RecordSchemaValidator {

  private final Map<String, JsonNode> streams;

  public RecordSchemaValidator(final StandardSyncInput syncInput) {
    final String streamPrefix = syncInput.getPrefix();

    // streams is Map of a stream name (including prefix) and stream schema
    // for easy access when we check each record's schema
    this.streams = syncInput.getCatalog().getStreams().stream().collect(
        Collectors.toMap(k -> String.format(streamPrefix + k.getStream().getName().trim()), v -> v.getStream().getJsonSchema()));
  }

  /**
   * Takes an AirbyteRecordMessage and uses the JsonSchemaValidator to validate that its data conforms
   * to the stream's schema If it does not, this method throws a RecordSchemaValidationException
   *
   * @param message
   * @throws RecordSchemaValidationException
   */
  public void validateSchema(final AirbyteRecordMessage message) throws RecordSchemaValidationException {
    // the stream this message corresponds to
    final String messageStream = message.getStream();
    final JsonNode messageData = message.getData();
    final JsonNode matchingSchema = streams.get(messageStream);

    final JsonSchemaValidator validator = new JsonSchemaValidator();

    // We must choose a JSON validator version for validating the schema
    // Rather than allowing connectors to use any version, we enforce validation using V7
    ((ObjectNode) matchingSchema).put("$schema", "http://json-schema.org/draft-07/schema#");

    try {
      validator.ensure(matchingSchema, messageData);
    } catch (final JsonValidationException e) {
      throw new RecordSchemaValidationException(String.format("Record schema validation failed. Errors: %s", e.getMessage()), e);
    }
  }

}

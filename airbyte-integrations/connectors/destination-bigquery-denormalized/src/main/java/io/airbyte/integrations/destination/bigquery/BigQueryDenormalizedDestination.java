/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Builder;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.base.Preconditions;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.bigquery.formatter.BigQueryRecordFormatter;
import io.airbyte.integrations.destination.bigquery.formatter.DefaultBigQueryDenormalizedRecordFormatter;
import io.airbyte.integrations.destination.bigquery.formatter.GcsBigQueryDenormalizedRecordFormatter;
import io.airbyte.integrations.destination.bigquery.uploader.UploaderType;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDenormalizedDestination extends BigQueryDestination {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDenormalizedDestination.class);

  public static final String NESTED_ARRAY_FIELD = "value";
  protected static final String PROPERTIES_FIELD = "properties";
  private static final String TYPE_FIELD = "type";
  private static final String FORMAT_FIELD = "format";
  private static final String REF_DEFINITION_KEY = "$ref";

  private final Set<String> fieldsContainRefDefinitionValue = new HashSet<>();

  @Override
  protected String getTargetTableName(final String streamName) {
    // This BigQuery destination does not write to a staging "raw" table but directly to a normalized
    // table
    return getNamingResolver().getIdentifier(streamName);
  }

  @Override
  protected Map<UploaderType, BigQueryRecordFormatter> getFormatterMap() {
    Map<UploaderType, BigQueryRecordFormatter> formatterMap = new HashMap<>();
    formatterMap.put(UploaderType.STANDARD, new DefaultBigQueryDenormalizedRecordFormatter(getNamingResolver(), fieldsContainRefDefinitionValue));
    formatterMap.put(UploaderType.AVRO, new GcsBigQueryDenormalizedRecordFormatter(getNamingResolver(), fieldsContainRefDefinitionValue));
    return formatterMap;  }

  @Override
  protected Schema getBigQuerySchema(final JsonNode jsonSchema) {
    final List<Field> fieldList = getSchemaFields(getNamingResolver(), jsonSchema);
    if (fieldList.stream().noneMatch(f -> f.getName().equals(JavaBaseConstants.COLUMN_NAME_AB_ID))) {
      fieldList.add(Field.of(JavaBaseConstants.COLUMN_NAME_AB_ID, StandardSQLTypeName.STRING));
    }
    if (fieldList.stream().noneMatch(f -> f.getName().equals(JavaBaseConstants.COLUMN_NAME_EMITTED_AT))) {
      fieldList.add(Field.of(JavaBaseConstants.COLUMN_NAME_EMITTED_AT, StandardSQLTypeName.TIMESTAMP));
    }
    LOGGER.info("Airbyte Schema is transformed from {} to {}.", jsonSchema, fieldList);
    return com.google.cloud.bigquery.Schema.of(fieldList);
  }

  private List<Field> getSchemaFields(final BigQuerySQLNameTransformer namingResolver, final JsonNode jsonSchema) {
    Preconditions.checkArgument(jsonSchema.isObject() && jsonSchema.has(PROPERTIES_FIELD));
    final ObjectNode properties = (ObjectNode) jsonSchema.get(PROPERTIES_FIELD);
    List<Field> tmpFields = Jsons.keys(properties).stream()
        .peek(addToRefList(properties))
        .map(key -> getField(namingResolver, key, properties.get(key))
            .build())
        .collect(Collectors.toList());
    if (!fieldsContainRefDefinitionValue.isEmpty()) {
      LOGGER.warn("Next fields contain \"$ref\" as Definition: {}. They are going to be saved as String Type column",
          fieldsContainRefDefinitionValue);
    }
    return tmpFields;
  }

  /**
   * @param properties - JSON schema with properties
   *
   *        The method is responsible for population of fieldsContainRefDefinitionValue set with keys
   *        contain $ref definition
   *
   *        Currently, AirByte doesn't support parsing value by $ref key definition. The issue to
   *        track this <a href="https://github.com/airbytehq/airbyte/issues/7725">7725</a>
   */
  private Consumer<String> addToRefList(ObjectNode properties) {
    return key -> {
      if (properties.get(key).has(REF_DEFINITION_KEY)) {
        fieldsContainRefDefinitionValue.add(key);
      }
    };
  }

  private static Builder getField(final BigQuerySQLNameTransformer namingResolver, final String key, final JsonNode fieldDefinition) {

    final String fieldName = namingResolver.getIdentifier(key);
    final Builder builder = Field.newBuilder(fieldName, StandardSQLTypeName.STRING);
    final List<JsonSchemaType> fieldTypes = getTypes(fieldName, fieldDefinition.get(TYPE_FIELD));
    for (int i = 0; i < fieldTypes.size(); i++) {
      final JsonSchemaType fieldType = fieldTypes.get(i);
      if (fieldType == JsonSchemaType.NULL) {
        builder.setMode(Mode.NULLABLE);
      }
      if (i == 0) {
        // Treat the first type in the list with the widest scope as the primary type
        final JsonSchemaType primaryType = fieldTypes.get(i);
        switch (primaryType) {
          case NULL -> {
            builder.setType(StandardSQLTypeName.STRING);
          }
          case STRING, NUMBER, INTEGER, BOOLEAN -> {
            builder.setType(primaryType.getBigQueryType());
          }
          case ARRAY -> {
            final JsonNode items;
            if (fieldDefinition.has("items")) {
              items = fieldDefinition.get("items");
            } else {
              LOGGER.warn("Source connector provided schema for ARRAY with missed \"items\", will assume that it's a String type");
              // this is handler for case when we get "array" without "items"
              // (https://github.com/airbytehq/airbyte/issues/5486)
              items = getTypeStringSchema();
            }
            final Builder subField = getField(namingResolver, fieldName, items).setMode(Mode.REPEATED);
            // "Array of Array of" (nested arrays) are not permitted by BigQuery ("Array of Record of Array of"
            // is)
            // Turn all "Array of" into "Array of Record of" instead
            return builder.setType(StandardSQLTypeName.STRUCT, subField.setName(NESTED_ARRAY_FIELD).build());
          }
          case OBJECT -> {
            final JsonNode properties;
            if (fieldDefinition.has(PROPERTIES_FIELD)) {
              properties = fieldDefinition.get(PROPERTIES_FIELD);
            } else {
              properties = fieldDefinition;
            }
            final FieldList fieldList = FieldList.of(Jsons.keys(properties)
                .stream()
                .map(f -> getField(namingResolver, f, properties.get(f)).build())
                .collect(Collectors.toList()));
            if (fieldList.size() > 0) {
              builder.setType(StandardSQLTypeName.STRUCT, fieldList);
            } else {
              builder.setType(StandardSQLTypeName.STRING);
            }
          }
          default -> {
            throw new IllegalStateException(
                String.format("Unexpected type for field %s: %s", fieldName, primaryType));
          }
        }
      }
    }

    // If a specific format is defined, use their specific type instead of the JSON's one
    final JsonNode fieldFormat = fieldDefinition.get(FORMAT_FIELD);
    if (fieldFormat != null) {
      final JsonSchemaFormat schemaFormat = JsonSchemaFormat.fromJsonSchemaFormat(fieldFormat.asText());
      if (schemaFormat != null) {
        builder.setType(schemaFormat.getBigQueryType());
      }
    }

    return builder;
  }

  private static JsonNode getTypeStringSchema() {
    return Jsons.deserialize("{\n"
        + "    \"type\": [\n"
        + "      \"string\"\n"
        + "    ]\n"
        + "  }");
  }

  private static List<JsonSchemaType> getTypes(final String fieldName, final JsonNode type) {
    if (type == null) {
      LOGGER.warn("Field {} has no type defined, defaulting to STRING", fieldName);
      return List.of(JsonSchemaType.STRING);
    } else if (type.isArray()) {
      return MoreIterators.toList(type.elements()).stream()
          .map(s -> JsonSchemaType.fromJsonSchemaType(s.asText()))
          // re-order depending to make sure wider scope types are first
          .sorted(Comparator.comparingInt(JsonSchemaType::getOrder))
          .collect(Collectors.toList());
    } else if (type.isTextual()) {
      return Collections.singletonList(JsonSchemaType.fromJsonSchemaType(type.asText()));
    } else {
      throw new IllegalStateException("Unexpected type: " + type);
    }
  }

  @Override
  protected boolean isDefaultAirbyteTmpTableSchema() {
    // Use source schema instead of
    return false;
  }

  public static void main(final String[] args) throws Exception {
    final Destination destination = new BigQueryDenormalizedDestination();
    LOGGER.info("starting destination: {}", BigQueryDenormalizedDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("completed destination: {}", BigQueryDenormalizedDestination.class);
  }

}

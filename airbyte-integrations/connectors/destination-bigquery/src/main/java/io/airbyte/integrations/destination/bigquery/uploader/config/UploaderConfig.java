package io.airbyte.integrations.destination.bigquery.uploader.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Schema;
import io.airbyte.integrations.destination.bigquery.formatter.BigQueryRecordFormatter;
import io.airbyte.integrations.destination.bigquery.uploader.UploaderType;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class UploaderConfig {

  private JsonNode config;
  private ConfiguredAirbyteStream configStream;
  private String targetTableName;
  private String tmpTableName;
  private BigQuery bigQuery;
  private Schema schema;
  private Map<UploaderType, BigQueryRecordFormatter> formatterMap;
  private boolean isDefaultAirbyteTmpSchema;

}

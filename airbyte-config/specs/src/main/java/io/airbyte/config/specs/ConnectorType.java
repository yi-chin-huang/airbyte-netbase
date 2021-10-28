package io.airbyte.config.specs;

public enum ConnectorType {

  SOURCE(
      "source_definitions.yaml",
      "source_definitions_specs.yaml"),
  DESTINATION(
      "destination_definitions.yaml",
      "destination_definitions_specs.yaml");

  private final String definitionFileName;
  private final String specFileName;

  ConnectorType(final String definitionFileName, final String specFileName) {
    this.definitionFileName = definitionFileName;
    this.specFileName = specFileName;
  }

  public String getDefinitionFileName() {
    return definitionFileName;
  }

  public String getSpecFileName() {
    return specFileName;
  }

}
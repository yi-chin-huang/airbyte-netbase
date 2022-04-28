import React from "react";
import { FormattedMessage } from "react-intl";

import { ConnectionConfiguration } from "core/domain/connection";
import { SourceDefinition, SourceDefinitionSpecification } from "core/domain/connector";
import { LogsRequestError } from "core/request/LogsRequestError";
import { useAnalyticsService } from "hooks/services/Analytics/useAnalyticsService";
import { createFormErrorMessage } from "utils/errorStatusMessage";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ServiceFormValues } from "views/Connector/ServiceForm/types";

type SourceFormProps = {
  onSubmit: (values: {
    name: string;
    serviceType: string;
    sourceDefinitionId?: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => void;
  afterSelectConnector?: () => void;
  sourceDefinitions: SourceDefinition[];
  hasSuccess?: boolean;
  error?: { message?: string; status?: number } | null;
  setSourceDefinitionId: React.Dispatch<React.SetStateAction<string | null>> | null;
  sourceDefinitionSpecification: SourceDefinitionSpecification | undefined;
  sourceDefinitionError: Error | null;
  isLoading: boolean;
};

const SourceForm: React.FC<SourceFormProps> = ({
  onSubmit,
  sourceDefinitions,
  error,
  hasSuccess,
  afterSelectConnector,
  setSourceDefinitionId,
  sourceDefinitionSpecification,
  sourceDefinitionError,
  isLoading,
}) => {
  const analyticsService = useAnalyticsService();

  //TODO: today's changes broke some functionality on the source creation form within the connectors setup flow if users are completing it there...
  const onDropDownSelect = (sourceDefinitionId: string) => {
    setSourceDefinitionId && setSourceDefinitionId(sourceDefinitionId);
    const connector = sourceDefinitions.find((item) => item.sourceDefinitionId === sourceDefinitionId);

    if (afterSelectConnector) {
      afterSelectConnector();
    }

    analyticsService.track("New Source - Action", {
      action: "Select a connector",
      connector_source_definition: connector?.name,
      connector_source_definition_id: sourceDefinitionId,
    });
  };

  const onSubmitForm = async (values: ServiceFormValues) => {
    await onSubmit({
      ...values,
      sourceDefinitionId: sourceDefinitionSpecification?.sourceDefinitionId,
    });
  };

  const errorMessage = error ? createFormErrorMessage(error) : null;

  return (
    <ConnectorCard
      onServiceSelect={onDropDownSelect}
      onSubmit={onSubmitForm}
      formType="source"
      availableServices={sourceDefinitions}
      selectedConnectorDefinitionSpecification={sourceDefinitionSpecification}
      hasSuccess={hasSuccess}
      fetchingConnectorError={sourceDefinitionError}
      errorMessage={errorMessage}
      isLoading={isLoading}
      formValues={
        sourceDefinitionSpecification
          ? { serviceType: sourceDefinitionSpecification.sourceDefinitionId, name: "" }
          : undefined
      }
      title={<FormattedMessage id="onboarding.sourceSetUp" />}
      jobInfo={LogsRequestError.extractJobInfo(error)}
    />
  );
};

export default SourceForm;

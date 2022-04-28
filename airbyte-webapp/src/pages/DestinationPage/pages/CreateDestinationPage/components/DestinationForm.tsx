import React from "react";
import { FormattedMessage } from "react-intl";

import { ConnectionConfiguration } from "core/domain/connection";
import { DestinationDefinition, DestinationDefinitionSpecification } from "core/domain/connector";
import { LogsRequestError } from "core/request/LogsRequestError";
import { useAnalyticsService } from "hooks/services/Analytics/useAnalyticsService";
import { createFormErrorMessage } from "utils/errorStatusMessage";
import { ConnectorCard } from "views/Connector/ConnectorCard";

type DestinationFormProps = {
  onSubmit: (values: {
    name: string;
    serviceType: string;
    destinationDefinitionId?: string;
    connectionConfiguration?: ConnectionConfiguration;
  }) => void;
  afterSelectConnector?: () => void;
  destinationDefinitions: DestinationDefinition[];
  hasSuccess?: boolean;
  error?: { message?: string; status?: number } | null;
  setDestinationDefinitionId: React.Dispatch<React.SetStateAction<string | null>> | null;
  destinationDefinitionSpecification: DestinationDefinitionSpecification | undefined;
  destinationDefinitionError: Error | null;
  isLoading: boolean;
};

// const hasDestinationDefinitionId = (state: unknown): state is { destinationDefinitionId: string } => {
//   return (
//     typeof state === "object" &&
//     state !== null &&
//     typeof (state as { destinationDefinitionId?: string }).destinationDefinitionId === "string"
//   );
// };

const DestinationForm: React.FC<DestinationFormProps> = ({
  onSubmit,
  destinationDefinitions,
  error,
  hasSuccess,
  afterSelectConnector,
  setDestinationDefinitionId,
  destinationDefinitionSpecification,
  destinationDefinitionError,
  isLoading,
}) => {
  const analyticsService = useAnalyticsService();

  const onDropDownSelect = (destinationDefinitionId: string) => {
    setDestinationDefinitionId && setDestinationDefinitionId(destinationDefinitionId);
    const connector = destinationDefinitions.find((item) => item.destinationDefinitionId === destinationDefinitionId);

    if (afterSelectConnector) {
      afterSelectConnector();
    }

    analyticsService.track("New Destination - Action", {
      action: "Select a connector",
      connector_destination_definition: connector?.name,
      connector_destination_definition_id: destinationDefinitionId,
    });
  };

  const onSubmitForm = async (values: { name: string; serviceType: string }) => {
    await onSubmit({
      ...values,
      destinationDefinitionId: destinationDefinitionSpecification?.destinationDefinitionId,
    });
  };

  const errorMessage = error ? createFormErrorMessage(error) : null;

  return (
    <ConnectorCard
      onServiceSelect={onDropDownSelect}
      fetchingConnectorError={destinationDefinitionError}
      onSubmit={onSubmitForm}
      formType="destination"
      availableServices={destinationDefinitions}
      selectedConnectorDefinitionSpecification={destinationDefinitionSpecification}
      hasSuccess={hasSuccess}
      errorMessage={errorMessage}
      isLoading={isLoading}
      formValues={
        destinationDefinitionSpecification
          ? { serviceType: destinationDefinitionSpecification.destinationDefinitionId }
          : undefined
      }
      title={<FormattedMessage id="onboarding.destinationSetUp" />}
      jobInfo={LogsRequestError.extractJobInfo(error)}
    />
  );
};

export default DestinationForm;

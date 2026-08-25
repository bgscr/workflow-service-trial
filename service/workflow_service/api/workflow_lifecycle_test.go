package api_test

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporalEnums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"github.com/sugerio/workflow-service-trial/service/workflow_service/api"
	workflowTemporal "github.com/sugerio/workflow-service-trial/service/workflow_service/temporal"
	"github.com/sugerio/workflow-service-trial/shared/structs"
	sharedTemporal "github.com/sugerio/workflow-service-trial/shared/temporal"
)

func TestRepeatedActivationIsIdempotent(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)

	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	assertActiveFormScheduleLifecycle(t, workflow)
	initialScheduleRunID := scheduleRunID(t, workflow)

	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	assertActiveFormScheduleLifecycle(t, workflow)
	require.Equal(t, initialScheduleRunID, scheduleRunID(t, workflow))
}

func TestRepeatedActivationIsIdempotentForWebhookOnlyWorkflow(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_execution_webhook_with_onReceived.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)

	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))

	current, err := api.GetWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	require.NoError(t, err)
	require.True(t, current.Active)
	nodeID, webhookID, err := api.GetWebhookIdAndNodeIdInWorkflow(current)
	require.NoError(t, err)
	webhooks, err := api.GetWebhookEntities(current.ID, webhookID)
	require.NoError(t, err)
	require.Len(t, webhooks, 1)
	require.Equal(t, http.MethodPost, webhooks[0].Method)
	response, err := api.CallWebhookFullResponse_Testing(
		testFiberLambda, http.MethodPost, current.ID, nodeID, webhookID, false, `{"data":"test"}`)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
}

func TestPreActiveWorkflowActivationMaterializesLifecycle(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule_active.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)
	require.True(t, workflow.Active)

	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	formNode := workflow.Nodes[0]
	webhooks, err := api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Empty(t, webhooks)
	_, err = temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(workflow.SugerOrgId, workflow.ID),
		"",
	)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)

	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	assertActiveFormScheduleLifecycle(t, workflow)
}

func TestRepeatedDeactivationIsIdempotent(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)

	t.Cleanup(func() {
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	require.NoError(t, api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	terminatedScheduleRunID := scheduleRunID(t, workflow)
	require.NoError(t, api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))

	current, err := api.GetWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	require.NoError(t, err)
	require.False(t, current.Active)
	var formNode *structs.WorkflowNode
	for index := range current.Nodes {
		if current.Nodes[index].Type == "n8n-nodes-base.formTrigger" {
			formNode = &current.Nodes[index]
			break
		}
	}
	require.NotNil(t, formNode)
	webhooks, err := api.GetWebhookEntities(current.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Empty(t, webhooks)

	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(current.SugerOrgId, current.ID),
		"",
	)
	require.NoError(t, err)
	require.NotNil(t, description.WorkflowExecutionInfo)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)
	require.Equal(t, terminatedScheduleRunID, description.WorkflowExecutionInfo.Execution.RunId)
}

func TestConcurrentActivationIsSerialized(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)

	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	const requestCount = 4
	start := make(chan struct{})
	errors := make(chan error, requestCount)
	var requests sync.WaitGroup
	for range requestCount {
		requests.Add(1)
		go func() {
			defer requests.Done()
			<-start
			errors <- api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		}()
	}
	close(start)
	requests.Wait()
	close(errors)

	for requestErr := range errors {
		assert.NoError(t, requestErr)
	}
	assertActiveFormScheduleLifecycle(t, workflow)
}

func TestFullWorkflowUpdateActivatesLifecycle(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)
	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	workflow.Name = "Activated by full update"
	workflow.Active = true
	updated, err := api.UpdateWorkflow_Testing(testFiberLambda, workflow)
	require.NoError(t, err)
	require.True(t, updated.Active)
	assertActiveFormScheduleLifecycle(t, workflow)
}

func TestFullWorkflowUpdateDeactivatesLifecycle(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)
	t.Cleanup(func() {
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	workflow, err = api.GetWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	require.NoError(t, err)
	workflow.Name = "Deactivated by full update"
	workflow.Active = false
	updated, err := api.UpdateWorkflow_Testing(testFiberLambda, workflow)
	require.NoError(t, err)
	require.False(t, updated.Active)

	var formNode *structs.WorkflowNode
	for index := range updated.Nodes {
		if updated.Nodes[index].Type == "n8n-nodes-base.formTrigger" {
			formNode = &updated.Nodes[index]
			break
		}
	}
	require.NotNil(t, formNode)
	webhooks, err := api.GetWebhookEntities(updated.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Empty(t, webhooks)
	getResponse, err := api.CallWebhookFullResponse_Testing(
		testFiberLambda, http.MethodGet, updated.ID, formNode.ID, formNode.WebhookId, false, "")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, getResponse.StatusCode)
	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(updated.SugerOrgId, updated.ID),
		"",
	)
	require.NoError(t, err)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)
}

func TestConcurrentFullUpdateAndActivationAreSerialized(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)
	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	fullUpdate := *workflow
	fullUpdate.Name = "Concurrent full update"
	fullUpdate.Active = true
	start := make(chan struct{})
	errors := make(chan error, 2)
	var requests sync.WaitGroup
	requests.Add(2)
	go func() {
		defer requests.Done()
		<-start
		_, updateErr := api.UpdateWorkflow_Testing(testFiberLambda, &fullUpdate)
		errors <- updateErr
	}()
	go func() {
		defer requests.Done()
		<-start
		errors <- api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	}()
	close(start)
	requests.Wait()
	close(errors)
	for requestErr := range errors {
		assert.NoError(t, requestErr)
	}
	assertActiveFormScheduleLifecycle(t, workflow)
}

func TestFullWorkflowActivationFailurePreservesInactiveLifecycle(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)
	t.Cleanup(func() {
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})
	installWebhookInsertFailure(t, workflow.ID)

	workflow.Name = "Fail full activation"
	workflow.Active = true
	_, err = api.UpdateWorkflow_Testing(testFiberLambda, workflow)
	require.Error(t, err)

	current, err := api.GetWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	require.NoError(t, err)
	require.False(t, current.Active)
	var formNode *structs.WorkflowNode
	for index := range current.Nodes {
		if current.Nodes[index].Type == "n8n-nodes-base.formTrigger" {
			formNode = &current.Nodes[index]
			break
		}
	}
	require.NotNil(t, formNode)
	webhooks, err := api.GetWebhookEntities(current.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Empty(t, webhooks)
	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(current.SugerOrgId, current.ID),
		"",
	)
	require.NoError(t, err)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)
}

func TestFullWorkflowUpdateRemovesScheduleWhileRemainingActive(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})
	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))

	workflow, err = api.GetWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	require.NoError(t, err)
	workflow.Name = "Remove schedule"
	workflow.Nodes = workflow.Nodes[:1]
	updated, err := api.UpdateWorkflow_Testing(testFiberLambda, workflow)
	require.NoError(t, err)
	require.True(t, updated.Active)
	formNode := updated.Nodes[0]
	webhooks, err := api.GetWebhookEntities(updated.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Len(t, webhooks, 2)
	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(updated.SugerOrgId, updated.ID),
		"",
	)
	require.NoError(t, err)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)
}

func TestFullWorkflowScheduleAdditionFailureRestoresSchedulelessActiveLifecycle(t *testing.T) {
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_execution_form_trigger_single.json")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})
	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	installWebhookInsertFailure(t, workflow.ID)

	workflow, err = api.GetWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	require.NoError(t, err)
	workflow.Name = "Fail adding schedule"
	workflow.Nodes = append(workflow.Nodes, structs.WorkflowNode{
		ID:          "added-schedule-trigger",
		Name:        "Schedule Trigger",
		Type:        "n8n-nodes-base.scheduleTrigger",
		TypeVersion: 1.1,
		Parameters: map[string]interface{}{
			"rule": map[string]interface{}{
				"interval": []interface{}{
					map[string]interface{}{"field": "hours", "hoursInterval": float64(24)},
				},
			},
		},
	})
	_, err = api.UpdateWorkflow_Testing(testFiberLambda, workflow)
	require.Error(t, err)

	current, err := api.GetWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	require.NoError(t, err)
	require.True(t, current.Active)
	require.Len(t, current.Nodes, 1)
	formNode := current.Nodes[0]
	webhooks, err := api.GetWebhookEntities(current.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Len(t, webhooks, 2)
	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(current.SugerOrgId, current.ID),
		"",
	)
	require.NoError(t, err)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)
}

func TestDeleteWorkflowProductionWebhookFailurePreservesLifecycle(t *testing.T) {
	workflow := createActiveWorkflowWithTestWebhooks(t)
	installWebhookDeleteFailure(t, workflow.ID, false)

	response, err := deleteWorkflow(t, workflow)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, response.StatusCode)

	assertCompleteLifecycleState(t, workflow, true)
}

func TestDeleteWorkflowTestWebhookFailurePreservesLifecycle(t *testing.T) {
	workflow := createActiveWorkflowWithTestWebhooks(t)
	installWebhookDeleteFailure(t, workflow.ID, true)

	response, err := deleteWorkflow(t, workflow)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, response.StatusCode)

	assertCompleteLifecycleState(t, workflow, true)
}

func TestDeleteWorkflowEntityFailurePreservesLifecycle(t *testing.T) {
	workflow := createActiveWorkflowWithTestWebhooks(t)
	installWorkflowEntityDeleteFailure(t, workflow.ID)

	response, err := deleteWorkflow(t, workflow)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, response.StatusCode)

	assertCompleteLifecycleState(t, workflow, true)
}

func TestDeleteWorkflowRemovesAllPersistentLifecycleState(t *testing.T) {
	workflow := createActiveWorkflowWithTestWebhooks(t)

	response, err := deleteWorkflow(t, workflow)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)

	_, err = api.GetWorkflow_Testing(testFiberLambda, workflow.SugerOrgId, workflow.ID)
	require.Error(t, err)
	webhooks, err := rdsDbQueries.QueryRowsWithCustomQuery(
		context.Background(),
		`SELECT "webhookPath" FROM workflow.webhook_entity WHERE "workflowId" = $1`,
		workflow.ID,
	)
	require.NoError(t, err)
	defer webhooks.Close()
	require.False(t, webhooks.Next())

	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(workflow.SugerOrgId, workflow.ID),
		"",
	)
	require.NoError(t, err)
	require.NotNil(t, description.WorkflowExecutionInfo)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)
}

func createActiveWorkflowWithTestWebhooks(t *testing.T) *structs.WorkflowEntity {
	t.Helper()
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "test_files/workflow_lifecycle_form_schedule.json")
	require.NoError(t, err)
	require.NotNil(t, workflow)
	t.Cleanup(func() {
		_ = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
		_ = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	})

	require.NoError(t, api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	manualRun, err := api.ManualRunWorkflowFullResponse_Testing(testFiberLambda, workflow)
	require.NoError(t, err)
	require.NotNil(t, manualRun.Data)
	require.True(t, manualRun.Data.WaitingForWebhook)
	testWebhookCleanupID := workflowTemporal.GetTemporalWorkflowOptions_UnregisterTestWebhooks(
		workflow.SugerOrgId, workflow.ID).ID
	require.NoError(t, sharedTemporal.TerminateWorkflowIfOpen(
		context.Background(), temporalClient, testWebhookCleanupID, "lifecycle test owns cleanup"))
	assertCompleteLifecycleState(t, workflow, true)
	return workflow
}

func installWebhookDeleteFailure(t *testing.T, workflowID string, testWebhook bool) {
	t.Helper()
	suffix := "production"
	pathPredicate := `RIGHT(OLD."webhookPath", 5) <> '/test'`
	if testWebhook {
		suffix = "test"
		pathPredicate = `RIGHT(OLD."webhookPath", 5) = '/test'`
	}
	identifier := fmt.Sprintf("fail_%s_%s", suffix, strings.ReplaceAll(workflowID, "-", "_"))
	functionName := identifier + "_fn"
	_, err := testRdsDb.Exec(fmt.Sprintf(`
		CREATE FUNCTION workflow.%s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD."workflowId" = '%s' AND %s THEN
				RAISE EXCEPTION 'forced %s webhook deletion failure';
			END IF;
			RETURN OLD;
		END;
		$$;
		CREATE TRIGGER %s BEFORE DELETE ON workflow.webhook_entity
		FOR EACH ROW EXECUTE FUNCTION workflow.%s();
	`, functionName, workflowID, pathPredicate, suffix, identifier, functionName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = testRdsDb.Exec(fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON workflow.webhook_entity; DROP FUNCTION IF EXISTS workflow.%s()",
			identifier,
			functionName,
		))
	})
}

func installWorkflowEntityDeleteFailure(t *testing.T, workflowID string) {
	t.Helper()
	identifier := "fail_workflow_" + strings.ReplaceAll(workflowID, "-", "_")
	functionName := identifier + "_fn"
	_, err := testRdsDb.Exec(fmt.Sprintf(`
		CREATE FUNCTION workflow.%s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.id = '%s' THEN
				RAISE EXCEPTION 'forced workflow entity deletion failure';
			END IF;
			RETURN OLD;
		END;
		$$;
		CREATE TRIGGER %s BEFORE DELETE ON workflow.workflow_entity
		FOR EACH ROW EXECUTE FUNCTION workflow.%s();
	`, functionName, workflowID, identifier, functionName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = testRdsDb.Exec(fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON workflow.workflow_entity; DROP FUNCTION IF EXISTS workflow.%s()",
			identifier,
			functionName,
		))
	})
}

func installWebhookInsertFailure(t *testing.T, workflowID string) {
	t.Helper()
	identifier := "fail_insert_" + strings.ReplaceAll(workflowID, "-", "_")
	functionName := identifier + "_fn"
	_, err := testRdsDb.Exec(fmt.Sprintf(`
		CREATE FUNCTION workflow.%s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW."workflowId" = '%s' AND NEW.method = 'POST' THEN
				RAISE EXCEPTION 'forced POST webhook insert failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER %s BEFORE INSERT ON workflow.webhook_entity
		FOR EACH ROW EXECUTE FUNCTION workflow.%s();
	`, functionName, workflowID, identifier, functionName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = testRdsDb.Exec(fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON workflow.webhook_entity; DROP FUNCTION IF EXISTS workflow.%s()",
			identifier,
			functionName,
		))
	})
}

func deleteWorkflow(t *testing.T, workflow *structs.WorkflowEntity) (events.APIGatewayProxyResponse, error) {
	t.Helper()
	return testFiberLambda.Proxy(events.APIGatewayProxyRequest{
		HTTPMethod:     http.MethodDelete,
		Path:           fmt.Sprintf("/workflow/org/%s/workflow/%s", workflow.SugerOrgId, workflow.ID),
		Headers:        map[string]string{"Content-Type": "application/json"},
		RequestContext: api.AuthorizerRequestContext,
	})
}

func assertCompleteLifecycleState(t *testing.T, workflow *structs.WorkflowEntity, active bool) {
	t.Helper()
	current, err := api.GetWorkflow_Testing(testFiberLambda, workflow.SugerOrgId, workflow.ID)
	require.NoError(t, err)
	require.Equal(t, active, current.Active)

	var formNode *structs.WorkflowNode
	for index := range current.Nodes {
		if current.Nodes[index].Type == "n8n-nodes-base.formTrigger" {
			formNode = &current.Nodes[index]
			break
		}
	}
	require.NotNil(t, formNode)
	webhooks, err := api.GetWebhookEntities(current.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Len(t, webhooks, 4)
	counts := map[string]int{}
	for _, webhook := range webhooks {
		counts[webhook.WebhookPath+":"+webhook.Method]++
	}
	require.Equal(t, 1, counts[formNode.WebhookId+":"+http.MethodGet])
	require.Equal(t, 1, counts[formNode.WebhookId+":"+http.MethodPost])
	require.Equal(t, 1, counts[formNode.WebhookId+"/test:"+http.MethodGet])
	require.Equal(t, 1, counts[formNode.WebhookId+"/test:"+http.MethodPost])

	for _, isTest := range []bool{false, true} {
		getResponse, err := api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodGet, current.ID, formNode.ID, formNode.WebhookId, isTest, "")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, getResponse.StatusCode)
		postResponse, err := api.CallWebhookWithContentTypeFullResponse_Testing(
			testFiberLambda,
			http.MethodPost,
			current.ID,
			formNode.ID,
			formNode.WebhookId,
			isTest,
			"application/x-www-form-urlencoded",
			"Name=Alice",
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, postResponse.StatusCode)
	}

	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(current.SugerOrgId, current.ID),
		"",
	)
	require.NoError(t, err)
	require.NotNil(t, description.WorkflowExecutionInfo)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_RUNNING, description.WorkflowExecutionInfo.Status)
}

func assertActiveFormScheduleLifecycle(t *testing.T, workflow *structs.WorkflowEntity) {
	t.Helper()

	current, err := api.GetWorkflow_Testing(testFiberLambda, workflow.SugerOrgId, workflow.ID)
	require.NoError(t, err)
	require.True(t, current.Active)

	var formNode *structs.WorkflowNode
	for index := range current.Nodes {
		if current.Nodes[index].Type == "n8n-nodes-base.formTrigger" {
			formNode = &current.Nodes[index]
			break
		}
	}
	require.NotNil(t, formNode)

	webhooks, err := api.GetWebhookEntities(current.ID, formNode.WebhookId)
	require.NoError(t, err)
	require.Len(t, webhooks, 2)
	methods := []string{webhooks[0].Method, webhooks[1].Method}
	sort.Strings(methods)
	require.Equal(t, []string{http.MethodGet, http.MethodPost}, methods)

	getResponse, err := api.CallWebhookFullResponse_Testing(
		testFiberLambda, http.MethodGet, current.ID, formNode.ID, formNode.WebhookId, false, "")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResponse.StatusCode)

	postResponse, err := api.CallWebhookWithContentTypeFullResponse_Testing(
		testFiberLambda,
		http.MethodPost,
		current.ID,
		formNode.ID,
		formNode.WebhookId,
		false,
		"application/x-www-form-urlencoded",
		"Name=Alice",
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, postResponse.StatusCode)

	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(current.SugerOrgId, current.ID),
		"",
	)
	require.NoError(t, err)
	require.NotNil(t, description.WorkflowExecutionInfo)
	require.Equal(t, temporalEnums.WORKFLOW_EXECUTION_STATUS_RUNNING, description.WorkflowExecutionInfo.Status)
}

func scheduleRunID(t *testing.T, workflow *structs.WorkflowEntity) string {
	t.Helper()
	description, err := temporalClient.DescribeWorkflowExecution(
		context.Background(),
		workflowTemporal.GetTemporalWorkflowId_ScheduleTrigger(workflow.SugerOrgId, workflow.ID),
		"",
	)
	require.NoError(t, err)
	require.NotNil(t, description.WorkflowExecutionInfo)
	require.NotNil(t, description.WorkflowExecutionInfo.Execution)
	return description.WorkflowExecutionInfo.Execution.RunId
}

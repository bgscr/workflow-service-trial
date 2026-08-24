package api_test

// Command to run this test only
// go test -v service/workflow_service/api/service_test.go service/workflow_service/api/webhook_test.go

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/sugerio/workflow-service-trial/service/workflow_service/api"
	_ "github.com/sugerio/workflow-service-trial/service/workflow_service/nodes/code"
	"github.com/sugerio/workflow-service-trial/shared/structs"
)

type WebhookTestSuite struct {
	suite.Suite
}

func Test_WebhookTestSuite(t *testing.T) {
	suite.Run(t, new(WebhookTestSuite))
}

func (s *WebhookTestSuite) TestHandleWebhook() {
	defaultRequestJson := "{\"msg\":\"content here\"}"

	s.T().Run("TestWebhook Call Webhook Mode of onReceived", func(t *testing.T) {
		t.Parallel()
		assert := require.New(s.T())
		// Create Organization for test
		organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
		// Create new workflow which contains a webhook of OnReceived mode.
		newWorkflow, err := api.CreateWorkflow_Testing(
			testFiberLambda, organization.ID, "./test_files/workflow_execution_webhook_with_onReceived.json")
		assert.Nil(err)
		assert.NotNil(newWorkflow)
		workflowId := newWorkflow.ID
		nodeId, webhookId, err := api.GetWebhookIdAndNodeIdInWorkflow(newWorkflow)
		assert.Nil(err)

		// Active the workflow
		err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflowId)
		assert.Nil(err)

		// Call webhook api (isTest=false)
		webhookResponse, err := api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodPost, workflowId, nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		// Check Result Body
		assert.Equal(200, webhookResponse.StatusCode)
		var webhookResponseBody map[string]interface{}
		err = json.Unmarshal([]byte(webhookResponse.Body), &webhookResponseBody)
		assert.Nil(err)
		assert.Equal("Workflow was started", webhookResponseBody["message"].(string))
		executionId := webhookResponseBody["executionId"].(string)
		assert.NotEmpty(executionId)
		// Check Result Header
		headerH1 := webhookResponse.MultiValueHeaders["H1"]
		assert.Equal(1, len(headerH1))
		assert.Equal("v1", headerH1[0])

		time.Sleep(3 * time.Second)

		// Get and Verify execution result
		execution, err := api.GetWorkflowExecution_Testing(testFiberLambda, organization.ID, executionId)
		assert.Nil(err)
		assert.NotNil(execution)
		assert.Equal(structs.WorkflowExecutionStatus_Success, execution.Status)
		assert.NotNil(execution.Data)
		assert.Equal(1, len(execution.Data.ResultData.RunData["Code"]))
		codeTaskResult := execution.Data.ResultData.RunData["Code"][0]
		assert.NotEmpty(codeTaskResult.Data["main"])
		assert.NotEmpty(codeTaskResult.Data["main"][0])
		codeResult := codeTaskResult.Data["main"][0][0]
		assert.NotEmpty(codeResult)
		codeResultJson, err := json.Marshal(codeResult["json"])
		assert.Nil(err)
		assert.Equal(defaultRequestJson, string(codeResultJson))

		// Call webhook api with invalid params
		// Invalid workflowId
		webhookResponse, err = api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodPost, "invalidWorkflowId", nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		assert.Equal(404, webhookResponse.StatusCode)
		// Invalid nodeId
		webhookResponse, err = api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodPost, workflowId, "invalidNodeId", webhookId, false, defaultRequestJson)
		assert.Nil(err)
		assert.Equal(404, webhookResponse.StatusCode)
		// Invalid webhookId
		webhookResponse, err = api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodPost, workflowId, nodeId, "invalidWebhookId", false, defaultRequestJson)
		assert.Nil(err)
		assert.Equal(404, webhookResponse.StatusCode)
		// Invalid httpMethod
		webhookResponse, err = api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodGet, workflowId, nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		assert.Equal(400, webhookResponse.StatusCode)

		// Deactive the workflow
		err = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflowId)
		assert.Nil(err)

		// Call webhook api again, result should be 404 because webhook has been deleted
		webhookResponse, err = api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodPost, workflowId, nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		assert.Equal(404, webhookResponse.StatusCode)

		// Delete workflow
		err = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflowId)
		assert.Nil(err)
	})

	s.T().Run("TestWebhook Call Webhook Mode of lastNode", func(t *testing.T) {
		t.Parallel()
		assert := require.New(s.T())
		// Create Organization for test
		organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
		// Create new workflow
		workflowEntity, err := api.CreateWorkflow_Testing(
			testFiberLambda, organization.ID, "./test_files/workflow_execution_webhook_with_lastNode.json")
		assert.Nil(err)
		assert.NotNil(workflowEntity)
		workflowId := workflowEntity.ID
		nodeId, webhookId, err := api.GetWebhookIdAndNodeIdInWorkflow(workflowEntity)
		assert.Nil(err)

		// Active the workflow
		err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflowId)
		assert.Nil(err)

		// Call webhook api (isTest=false)
		webhookResponse, err := api.CallWebhook_Testing(
			testFiberLambda, http.MethodPost, workflowId, nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		webhookResponseJson, err := json.Marshal(webhookResponse)
		assert.Nil(err)
		assert.Equal(defaultRequestJson, string(webhookResponseJson))
	})

	// When a webhook use mode of responseNode, the request will return after the respondToWebhook node executed.
	s.T().Run("TestWebhook Call Webhook Mode of responseNode", func(t *testing.T) {
		t.Parallel()
		assert := require.New(s.T())
		// Create Organization for test
		organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
		// Create new workflow
		workflowEntity, err := api.CreateWorkflow_Testing(
			testFiberLambda, organization.ID, "./test_files/workflow_execution_webhook_with_responseNode.json")
		assert.Nil(err)
		assert.NotNil(workflowEntity)
		workflowId := workflowEntity.ID
		nodeId, webhookId, err := api.GetWebhookIdAndNodeIdInWorkflow(workflowEntity)
		assert.Nil(err)

		// Active the workflow
		err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflowId)
		assert.Nil(err)

		// Call webhook api (isTest=false)
		webhookResponse, err := api.CallWebhook_Testing(
			testFiberLambda, http.MethodPost, workflowId, nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		webhookResponseJson, err := json.Marshal(webhookResponse)
		assert.Nil(err)
		assert.Equal(defaultRequestJson, string(webhookResponseJson))
	})

	// When a webhook use mode of responseNode but there is no respondToWebhook node
	// the request will wait until the workflow last node executed
	s.T().Run("TestWebhook Call Webhook Mode of responseNode absent", func(t *testing.T) {
		t.Parallel()
		assert := require.New(s.T())
		// Create Organization for test
		organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
		// Create new workflow
		workflowEntity, err := api.CreateWorkflow_Testing(
			testFiberLambda, organization.ID, "./test_files/workflow_execution_webhook_with_responseNode_absent.json")
		assert.Nil(err)
		assert.NotNil(workflowEntity)
		workflowId := workflowEntity.ID
		nodeId, webhookId, err := api.GetWebhookIdAndNodeIdInWorkflow(workflowEntity)
		assert.Nil(err)

		// Active the workflow
		err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflowId)
		assert.Nil(err)

		// Call webhook api (isTest=false)
		webhookResponse, err := api.CallWebhook_Testing(
			testFiberLambda, http.MethodPost, workflowId, nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		assert.Equal("Workflow executed successfully", webhookResponse["message"].(string))
		executionId := webhookResponse["executionId"].(string)
		assert.NotEmpty(executionId)
	})

	s.T().Run("TestWebhook Call Webhook Mode of lastNode Execution Failed", func(t *testing.T) {
		t.Parallel()
		assert := require.New(s.T())
		// Create Organization for test
		organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
		// Create new workflow
		workflowEntity, err := api.CreateWorkflow_Testing(
			testFiberLambda, organization.ID, "./test_files/workflow_execution_webhook_with_lastNode_failed.json")
		assert.Nil(err)
		assert.NotNil(workflowEntity)
		workflowId := workflowEntity.ID
		nodeId, webhookId, err := api.GetWebhookIdAndNodeIdInWorkflow(workflowEntity)
		assert.Nil(err)

		// Active the workflow
		err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflowId)
		assert.Nil(err)

		// Call webhook api (isTest=false)
		webhookResponse, err := api.CallWebhookFullResponse_Testing(
			testFiberLambda, http.MethodPost, workflowId, nodeId, webhookId, false, defaultRequestJson)
		assert.Nil(err)
		// Verify result, code should be 500 because the workflow execute should got failed.
		assert.Equal(500, webhookResponse.StatusCode)
		var webhookResponseBody map[string]interface{}
		err = json.Unmarshal([]byte(webhookResponse.Body), &webhookResponseBody)
		assert.Nil(err)
		assert.Equal(
			"TypeError: Cannot read property 'split' of undefined or null [line 2]",
			webhookResponseBody["message"].(string))
	})
}

func (s *WebhookTestSuite) TestFormTriggerGET() {
	assert := require.New(s.T())
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "./test_files/workflow_execution_form_trigger_single.json")
	assert.NoError(err)
	assert.NotNil(workflow)

	err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	assert.NoError(err)
	deleted := false
	s.T().Cleanup(func() {
		if !deleted {
			assert.NoError(api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
		}
	})

	formNode := workflow.Nodes[0]
	entities, err := api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	assert.NoError(err)
	methods := make([]string, 0, len(entities))
	for _, entity := range entities {
		methods = append(methods, entity.Method)
	}
	sort.Strings(methods)
	assert.Equal([]string{http.MethodGet, http.MethodPost}, methods)

	countBefore, err := rdsDbQueries.CountWorkflowExecutionEntitiesByWorkflowId(context.Background(), workflow.ID)
	assert.NoError(err)
	response, err := api.CallWebhookFullResponse_Testing(
		testFiberLambda, http.MethodGet, workflow.ID, formNode.ID, formNode.WebhookId, false, "")
	assert.NoError(err)
	assert.Equal(http.StatusOK, response.StatusCode)
	assert.Contains(response.MultiValueHeaders["Content-Type"], "text/html; charset=utf-8")

	document, err := goquery.NewDocumentFromReader(bytes.NewBufferString(response.Body))
	assert.NoError(err)
	assert.Equal("Contact us", document.Find("title").Text())
	assert.Equal("Contact us", document.Find("h1").Text())
	assert.Equal(
		"We will get back to you soon. <script>alert(\"description\")</script>",
		document.Find("p").First().Text(),
	)
	assert.Zero(document.Find("script").Length())
	form := document.Find("form")
	assert.Equal("post", form.AttrOr("method", ""))
	assert.Contains(form.AttrOr("action", ""), "webhookId="+formNode.WebhookId)
	assert.Contains(form.AttrOr("action", ""), "isTest=false")
	assert.Equal("text", document.Find(`input[name="field-0"]`).AttrOr("type", ""))
	assert.Equal("email", document.Find(`input[name="field-1"]`).AttrOr("type", ""))
	assert.Equal(2, document.Find(`select[name="field-2"] option`).Length())

	countAfter, err := rdsDbQueries.CountWorkflowExecutionEntitiesByWorkflowId(context.Background(), workflow.ID)
	assert.NoError(err)
	assert.Equal(countBefore, countAfter)

	response, err = api.CallWebhookFullResponse_Testing(
		testFiberLambda, http.MethodPut, workflow.ID, formNode.ID, formNode.WebhookId, false, "")
	assert.NoError(err)
	assert.Equal(http.StatusBadRequest, response.StatusCode)

	err = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	assert.NoError(err)
	entities, err = api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	assert.NoError(err)
	assert.Empty(entities)

	err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	assert.NoError(err)
	err = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	assert.NoError(err)
	deleted = true
	entities, err = api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	assert.NoError(err)
	assert.Empty(entities)
}

func (s *WebhookTestSuite) TestFormTriggerActivationRollsBackWhenSecondWebhookInsertFails() {
	t := s.T()
	assert := require.New(t)
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "./test_files/workflow_execution_form_trigger_single.json")
	assert.NoError(err)
	assert.NotNil(workflow)
	t.Cleanup(func() {
		assert.NoError(api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	})

	_, err = testRdsDb.ExecContext(context.Background(), `
		CREATE FUNCTION workflow.fail_form_trigger_post_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.method = 'POST' THEN
				RAISE EXCEPTION 'forced POST webhook insert failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER fail_form_trigger_post_insert
		BEFORE INSERT ON workflow.webhook_entity
		FOR EACH ROW EXECUTE FUNCTION workflow.fail_form_trigger_post_insert();
	`)
	assert.NoError(err)
	t.Cleanup(func() {
		_, cleanupErr := testRdsDb.ExecContext(context.Background(), `
			DROP TRIGGER fail_form_trigger_post_insert ON workflow.webhook_entity;
			DROP FUNCTION workflow.fail_form_trigger_post_insert();
		`)
		assert.NoError(cleanupErr)
	})

	err = api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	assert.Error(err)

	formNode := workflow.Nodes[0]
	entities, err := api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	assert.NoError(err)
	assert.Empty(entities)
}

func (s *WebhookTestSuite) TestFormTriggerDeactivationRollsBackWhenSecondWebhookDeleteFails() {
	t := s.T()
	assert := require.New(t)
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "./test_files/workflow_execution_form_trigger_single.json")
	assert.NoError(err)
	assert.NotNil(workflow)
	t.Cleanup(func() {
		assert.NoError(api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	})
	assert.NoError(api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))

	formNode := workflow.Nodes[0]
	entities, err := api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	assert.NoError(err)
	assert.Len(entities, 2)

	_, err = testRdsDb.ExecContext(context.Background(), `
		CREATE FUNCTION workflow.fail_form_trigger_post_delete() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.method = 'POST' THEN
				RAISE EXCEPTION 'forced POST webhook delete failure';
			END IF;
			RETURN OLD;
		END;
		$$;
		CREATE TRIGGER fail_form_trigger_post_delete
		BEFORE DELETE ON workflow.webhook_entity
		FOR EACH ROW EXECUTE FUNCTION workflow.fail_form_trigger_post_delete();
	`)
	assert.NoError(err)
	t.Cleanup(func() {
		_, cleanupErr := testRdsDb.ExecContext(context.Background(), `
			DROP TRIGGER fail_form_trigger_post_delete ON workflow.webhook_entity;
			DROP FUNCTION workflow.fail_form_trigger_post_delete();
		`)
		assert.NoError(cleanupErr)
	})

	err = api.DeactivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	assert.Error(err)
	entities, err = api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	assert.NoError(err)
	methods := make([]string, 0, len(entities))
	for _, entity := range entities {
		methods = append(methods, entity.Method)
	}
	sort.Strings(methods)
	assert.Equal([]string{http.MethodGet, http.MethodPost}, methods)
}

func (s *WebhookTestSuite) TestFormTriggerWorkflowDeletionFailsWhenWebhookDeletionFails() {
	t := s.T()
	assert := require.New(t)
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "./test_files/workflow_execution_form_trigger_single.json")
	assert.NoError(err)
	assert.NotNil(workflow)
	workflowDeleted := false
	t.Cleanup(func() {
		if !workflowDeleted {
			assert.NoError(api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
		}
	})
	assert.NoError(api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))

	_, err = testRdsDb.ExecContext(context.Background(), `
		CREATE FUNCTION workflow.fail_form_trigger_post_delete() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.method = 'POST' THEN
				RAISE EXCEPTION 'forced POST webhook delete failure';
			END IF;
			RETURN OLD;
		END;
		$$;
		CREATE TRIGGER fail_form_trigger_post_delete
		BEFORE DELETE ON workflow.webhook_entity
		FOR EACH ROW EXECUTE FUNCTION workflow.fail_form_trigger_post_delete();
	`)
	assert.NoError(err)
	t.Cleanup(func() {
		_, cleanupErr := testRdsDb.ExecContext(context.Background(), `
			DROP TRIGGER fail_form_trigger_post_delete ON workflow.webhook_entity;
			DROP FUNCTION workflow.fail_form_trigger_post_delete();
		`)
		assert.NoError(cleanupErr)
	})

	err = api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID)
	assert.Error(err)
	formNode := workflow.Nodes[0]
	entities, err := api.GetWebhookEntities(workflow.ID, formNode.WebhookId)
	assert.NoError(err)
	methods := make([]string, 0, len(entities))
	for _, entity := range entities {
		methods = append(methods, entity.Method)
	}
	sort.Strings(methods)
	assert.Equal([]string{http.MethodGet, http.MethodPost}, methods)
}

func (s *WebhookTestSuite) TestFormTriggerPOSTExecutesSingleNodeWorkflow() {
	assert := require.New(s.T())
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "./test_files/workflow_execution_form_trigger_single.json")
	assert.NoError(err)
	assert.NotNil(workflow)
	s.T().Cleanup(func() {
		assert.NoError(api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	})
	assert.NoError(api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))

	formNode := workflow.Nodes[0]
	response, err := api.CallWebhookWithContentTypeFullResponse_Testing(
		testFiberLambda,
		http.MethodPost,
		workflow.ID,
		formNode.ID,
		formNode.WebhookId,
		false,
		"application/x-www-form-urlencoded",
		"field-0=SubscriptionConfirmation&field-1=++alice%40example.com++&field-2=Support",
	)
	assert.NoError(err)
	assert.Equal(http.StatusOK, response.StatusCode)
	responseBody := map[string]string{}
	assert.NoError(json.Unmarshal([]byte(response.Body), &responseBody))
	assert.Equal("Workflow was started", responseBody["message"])
	executionID := responseBody["executionId"]
	assert.NotEmpty(executionID)

	var execution *structs.WorkflowExecution
	assert.Eventually(func() bool {
		execution, err = api.GetWorkflowExecution_Testing(testFiberLambda, organization.ID, executionID)
		return err == nil && execution.Status == structs.WorkflowExecutionStatus_Success
	}, 10*time.Second, 100*time.Millisecond)

	formRuns := execution.Data.ResultData.RunData["Form Trigger"]
	assert.Len(formRuns, 1)
	output := formRuns[0].Data["main"][0][0]["json"].(map[string]interface{})
	assert.Equal("SubscriptionConfirmation", output["Name"])
	assert.Equal("alice@example.com", output["Email"])
	assert.Equal("Support", output["Department"])
	assert.Contains(output, "Optional note")
	assert.Nil(output["Optional note"])
	assert.Equal("production", output["formMode"])
	_, err = time.Parse(time.RFC3339, output["submittedAt"].(string))
	assert.NoError(err)
}

func (s *WebhookTestSuite) TestFormTriggerPOSTPropagatesToDownstreamNode() {
	assert := require.New(s.T())
	organization := structs.CreateOrganization_Testing(rdsDbQueries, sid, "")
	workflow, err := api.CreateWorkflow_Testing(
		testFiberLambda, organization.ID, "./test_files/workflow_execution_form_trigger_downstream.json")
	assert.NoError(err)
	assert.NotNil(workflow)
	s.T().Cleanup(func() {
		assert.NoError(api.DeleteWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))
	})
	assert.NoError(api.ActivateWorkflow_Testing(testFiberLambda, organization.ID, workflow.ID))

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	assert.NoError(writer.WriteField("field-0", "SubscriptionConfirmation"))
	assert.NoError(writer.WriteField("field-1", "Support"))
	assert.NoError(writer.Close())
	formNode := workflow.Nodes[0]
	response, err := api.CallWebhookWithContentTypeFullResponse_Testing(
		testFiberLambda,
		http.MethodPost,
		workflow.ID,
		formNode.ID,
		formNode.WebhookId,
		false,
		writer.FormDataContentType(),
		body.String(),
	)
	assert.NoError(err)
	assert.Equal(http.StatusOK, response.StatusCode)
	responseBody := map[string]string{}
	assert.NoError(json.Unmarshal([]byte(response.Body), &responseBody))
	executionID := responseBody["executionId"]
	assert.NotEmpty(executionID)

	var execution *structs.WorkflowExecution
	assert.Eventually(func() bool {
		execution, err = api.GetWorkflowExecution_Testing(testFiberLambda, organization.ID, executionID)
		return err == nil && execution.Status == structs.WorkflowExecutionStatus_Success
	}, 10*time.Second, 100*time.Millisecond)

	codeRuns := execution.Data.ResultData.RunData["Code"]
	assert.Len(codeRuns, 1)
	output := codeRuns[0].Data["main"][0][0]["json"].(map[string]interface{})
	assert.Equal("SubscriptionConfirmation", output["propagatedName"])
	assert.Equal("Support", output["propagatedDepartment"])
	assert.Equal("production", output["propagatedMode"])
}

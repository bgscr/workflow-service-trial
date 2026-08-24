package form_trigger_test

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/stretchr/testify/require"
	"github.com/sugerio/workflow-service-trial/service/workflow_service/core"
	_ "github.com/sugerio/workflow-service-trial/service/workflow_service/nodes"
	"github.com/sugerio/workflow-service-trial/shared/structs"
	"github.com/valyala/fasthttp"
)

func TestFormTriggerIsRegisteredWithWebhooksAndIcon(t *testing.T) {
	const nodeName = "n8n-nodes-base.formTrigger"

	node, ok := core.GetAllNodeObjects()[nodeName]
	require.True(t, ok)

	spec, ok := node.DefaultSpec().(*structs.WorkflowNodeSpec)
	require.True(t, ok)
	require.Equal(t, "n8n Form Trigger", spec.NodeSpec.DisplayName)
	require.Equal(t, "/icons/embed/n8n-nodes-base.formTrigger/form.svg", spec.NodeSpec.IconUrl)

	methods := make(map[string]bool, len(spec.NodeSpec.Webhooks))
	for _, webhook := range spec.NodeSpec.Webhooks {
		methods[webhook.HttpMethod] = true
	}
	require.Equal(t, map[string]bool{
		http.MethodGet:  true,
		http.MethodPost: true,
	}, methods)

	require.NotEmpty(t, core.GetAllNodeEmbedIcons()[nodeName])
}

func TestFormTriggerDiscoversOneWebhookPerDeclaredMethod(t *testing.T) {
	workflow := &structs.WorkflowEntity{
		ID: "workflow-id",
		Nodes: []structs.WorkflowNode{{
			ID:        "node-id",
			Name:      "Form Trigger",
			Type:      "n8n-nodes-base.formTrigger",
			WebhookId: "webhook-id",
		}},
	}

	webhooks := core.GetWorkflowWebhooks(workflow, false)
	require.Len(t, webhooks, 2)

	methods := []string{webhooks[0].HttpMethod, webhooks[1].HttpMethod}
	sort.Strings(methods)
	require.Equal(t, []string{http.MethodGet, http.MethodPost}, methods)
	for _, webhook := range webhooks {
		require.Equal(t, webhook.HttpMethod, webhook.WebhookDescription.HttpMethod)
	}
}

func TestWebhookDiscoveryStillUsesParameterDerivedMethod(t *testing.T) {
	workflow := &structs.WorkflowEntity{
		ID: "workflow-id",
		Nodes: []structs.WorkflowNode{{
			ID:         "node-id",
			Name:       "Webhook",
			Type:       "n8n-nodes-base.webhook",
			WebhookId:  "webhook-id",
			Parameters: map[string]interface{}{"httpMethod": http.MethodPatch},
		}},
	}

	webhooks := core.GetWorkflowWebhooks(workflow, false)
	require.Len(t, webhooks, 1)
	require.Equal(t, http.MethodPatch, webhooks[0].HttpMethod)
}

func TestFormTriggerRendersConfiguredEscapedHTML(t *testing.T) {
	nodeObject := core.GetAllNodeObjects()["n8n-nodes-base.formTrigger"]
	renderer, ok := nodeObject.(core.NodeFormRenderer)
	require.True(t, ok)

	formNode := &structs.WorkflowNode{Parameters: map[string]interface{}{
		"formTitle":       `<script>alert("title")</script>`,
		"formDescription": `<img src=x onerror="alert('description')">`,
		"formFields": map[string]interface{}{"values": []interface{}{
			map[string]interface{}{
				"fieldLabel":    `Name <strong>required</strong>`,
				"requiredField": true,
			},
			map[string]interface{}{
				"fieldLabel": `Email "address"`,
				"fieldType":  "email",
			},
			map[string]interface{}{
				"fieldLabel": `Choose <one>`,
				"fieldType":  "dropdown",
				"fieldOptions": map[string]interface{}{"values": []interface{}{
					map[string]interface{}{"option": `<script>alert("option")</script>`},
				}},
			},
		}},
	}}
	action := `/workflow/public/webhook/workflow/workflow-id/node/node-id?webhookId=id%22%3E%3Cscript%3E&isTest=true`

	html, err := renderer.RenderForm(formNode, action)
	require.NoError(t, err)
	document, err := goquery.NewDocumentFromReader(bytes.NewReader(html))
	require.NoError(t, err)

	require.Equal(t, `<script>alert("title")</script>`, document.Find("title").Text())
	require.Equal(t, `<script>alert("title")</script>`, document.Find("h1").Text())
	require.Equal(t, `<img src=x onerror="alert('description')">`, document.Find("p").First().Text())
	require.Equal(t, 0, document.Find("script, img, strong, one").Length())

	form := document.Find("form")
	require.Equal(t, "post", form.AttrOr("method", ""))
	require.Equal(t, action, form.AttrOr("action", ""))

	textInput := document.Find(`input[name="field-0"]`)
	require.Equal(t, "text", textInput.AttrOr("type", ""))
	_, required := textInput.Attr("required")
	require.True(t, required)
	require.Equal(t, `Name <strong>required</strong>`, document.Find(`label[for="field-0"]`).Text())

	require.Equal(t, "email", document.Find(`input[name="field-1"]`).AttrOr("type", ""))
	selectInput := document.Find(`select[name="field-2"]`)
	require.Equal(t, `<script>alert("option")</script>`, selectInput.Find("option").Text())
}

func TestFormTriggerParsesURLAndMultipartFormSubmissions(t *testing.T) {
	formNode := &structs.WorkflowNode{Parameters: map[string]interface{}{
		"formFields": map[string]interface{}{"values": []interface{}{
			map[string]interface{}{"fieldLabel": "Name"},
			map[string]interface{}{"fieldLabel": "Email", "fieldType": "email"},
			map[string]interface{}{"fieldLabel": "Department", "fieldType": "dropdown"},
			map[string]interface{}{
				"fieldLabel":    "Missing required",
				"fieldType":     "text",
				"requiredField": true,
			},
		}},
	}}

	tests := []struct {
		name        string
		isTestValue string
		request     func(t *testing.T) *fasthttp.Request
		formMode    string
	}{
		{
			name:        "URL encoded production submission",
			isTestValue: "false",
			formMode:    "production",
			request: func(t *testing.T) *fasthttp.Request {
				request := &fasthttp.Request{}
				request.Header.SetContentType("application/x-www-form-urlencoded")
				request.SetBodyString("field-0=++Alice++&field-1=++alice%40example.com++&field-2=+Support+")
				return request
			},
		},
		{
			name:        "multipart test submission",
			isTestValue: "TRUE",
			formMode:    "test",
			request: func(t *testing.T) *fasthttp.Request {
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				require.NoError(t, writer.WriteField("field-0", "  Alice  "))
				require.NoError(t, writer.WriteField("field-1", "  alice@example.com  "))
				require.NoError(t, writer.WriteField("field-2", " Support "))
				require.NoError(t, writer.Close())

				request := &fasthttp.Request{}
				request.Header.SetContentType(writer.FormDataContentType())
				request.SetBody(body.Bytes())
				return request
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := test.request(t)
			request.URI().QueryArgs().Set("isTest", test.isTestValue)
			result := core.GetAllNodeObjects()["n8n-nodes-base.formTrigger"].Execute(
				context.Background(),
				&structs.NodeExecuteInput{
					Params: formNode,
					AdditionalData: &structs.WorkflowExecuteAdditionalData{
						HttpRequest: request,
					},
				},
			)

			require.Equal(t, structs.WorkflowExecutionStatus_Success, result.ExecutionStatus)
			require.Len(t, result.TriggerData, 1)
			output, ok := result.TriggerData[0]["json"].(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, "Alice", output["Name"])
			require.Equal(t, "alice@example.com", output["Email"])
			require.Equal(t, " Support ", output["Department"])
			require.Contains(t, output, "Missing required")
			require.Nil(t, output["Missing required"])
			require.Equal(t, test.formMode, output["formMode"])
			_, err := time.Parse(time.RFC3339, output["submittedAt"].(string))
			require.NoError(t, err)
		})
	}
}

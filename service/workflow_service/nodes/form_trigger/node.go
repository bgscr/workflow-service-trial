package form_trigger

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"mime"
	"mime/multipart"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sugerio/workflow-service-trial/service/workflow_service/core"
	"github.com/sugerio/workflow-service-trial/shared/structs"
	"github.com/valyala/fasthttp"
)

const (
	Category = structs.CategoryTrigger
	Name     = "n8n-nodes-base.formTrigger"
)

var (
	//go:embed node.json
	rawJSON []byte

	//go:embed form.svg
	icon []byte
)

type FormTrigger struct {
	spec *structs.WorkflowNodeSpec
}

type formConfiguration struct {
	Title       string     `json:"formTitle"`
	Description string     `json:"formDescription"`
	Fields      formFields `json:"formFields"`
}

type formFields struct {
	Values []formField `json:"values"`
}

type formField struct {
	Label      string           `json:"fieldLabel"`
	Type       string           `json:"fieldType"`
	Options    formFieldOptions `json:"fieldOptions"`
	IsRequired bool             `json:"requiredField"`
}

type formFieldOptions struct {
	Values []formFieldOption `json:"values"`
}

type formFieldOption struct {
	Value string `json:"option"`
}

type formTemplateData struct {
	Title       string
	Description string
	ActionURL   string
	Fields      []formTemplateField
}

type formTemplateField struct {
	ID         string
	Label      string
	Type       string
	Options    []formFieldOption
	IsRequired bool
}

var formTemplate = template.Must(template.New("form").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{.Title}}</title>
</head>
<body>
  <main>
    <h1>{{.Title}}</h1>
    <p>{{.Description}}</p>
    <form method="post" action="{{.ActionURL}}">
      {{range .Fields}}
      <div>
        <label for="{{.ID}}">{{.Label}}</label>
        {{if eq .Type "dropdown"}}
        <select id="{{.ID}}" name="{{.ID}}"{{if .IsRequired}} required{{end}}>
          {{range .Options}}<option value="{{.Value}}">{{.Value}}</option>{{end}}
        </select>
        {{else}}
        <input id="{{.ID}}" name="{{.ID}}" type="{{.Type}}"{{if .IsRequired}} required{{end}}>
        {{end}}
      </div>
      {{end}}
      <button type="submit">Submit</button>
    </form>
  </main>
</body>
</html>`))

func init() {
	trigger := &FormTrigger{spec: &structs.WorkflowNodeSpec{}}
	trigger.spec.JsonConfig = rawJSON
	trigger.spec.GenerateSpec()
	core.Register(trigger)
	core.RegisterEmbedIcons(Name, icon)
}

func (trigger *FormTrigger) Category() structs.NodeObjectCategory {
	return Category
}

func (trigger *FormTrigger) Name() string {
	return Name
}

func (trigger *FormTrigger) DefaultSpec() interface{} {
	return trigger.spec
}

func (trigger *FormTrigger) RenderForm(node *structs.WorkflowNode, actionURL string) ([]byte, error) {
	configuration, err := parseFormConfiguration(node)
	if err != nil {
		return nil, err
	}

	fields := make([]formTemplateField, 0, len(configuration.Fields.Values))
	for index, field := range configuration.Fields.Values {
		fieldType := "text"
		if field.Type == "email" || field.Type == "dropdown" {
			fieldType = field.Type
		}
		fields = append(fields, formTemplateField{
			ID:         fmt.Sprintf("field-%d", index),
			Label:      field.Label,
			Type:       fieldType,
			Options:    field.Options.Values,
			IsRequired: field.IsRequired,
		})
	}

	var output bytes.Buffer
	err = formTemplate.Execute(&output, formTemplateData{
		Title:       configuration.Title,
		Description: configuration.Description,
		ActionURL:   actionURL,
		Fields:      fields,
	})
	if err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func parseFormConfiguration(node *structs.WorkflowNode) (*formConfiguration, error) {
	if node == nil {
		return nil, errors.New("form trigger node is nil")
	}
	data, err := json.Marshal(node.Parameters)
	if err != nil {
		return nil, err
	}
	configuration := &formConfiguration{}
	if err := json.Unmarshal(data, configuration); err != nil {
		return nil, err
	}
	return configuration, nil
}

func (trigger *FormTrigger) Execute(_ context.Context, input *structs.NodeExecuteInput) *structs.NodeExecutionResult {
	if input == nil || input.AdditionalData == nil || input.AdditionalData.HttpRequest == nil {
		return core.GenerateFailedResponse(Name, errors.New("form submission request is missing"))
	}
	configuration, err := parseFormConfiguration(input.Params)
	if err != nil {
		return core.GenerateFailedResponse(Name, err)
	}
	values, err := parseSubmission(input.AdditionalData.HttpRequest)
	if err != nil {
		return core.GenerateFailedResponse(Name, err)
	}

	output := make(map[string]interface{}, len(configuration.Fields.Values)+2)
	for index, field := range configuration.Fields.Values {
		value, present := values[fmt.Sprintf("field-%d", index)]
		if !present {
			output[field.Label] = nil
			continue
		}
		if field.Type == "" || field.Type == "text" || field.Type == "email" {
			value = strings.TrimSpace(value)
		}
		output[field.Label] = value
	}
	output["submittedAt"] = time.Now().UTC().Format(time.RFC3339)
	formMode := "production"
	isTest, _ := strconv.ParseBool(string(input.AdditionalData.HttpRequest.URI().QueryArgs().Peek("isTest")))
	if isTest {
		formMode = "test"
	}
	output["formMode"] = formMode

	return core.GenerateSuccessResponse(structs.NodeData{{"json": output}}, []structs.NodeData{})
}

func parseSubmission(request *fasthttp.Request) (map[string]string, error) {
	mediaType, parameters, err := mime.ParseMediaType(string(request.Header.ContentType()))
	if err != nil {
		return nil, err
	}

	values := map[string]string{}
	switch mediaType {
	case "application/x-www-form-urlencoded":
		parsed, err := url.ParseQuery(string(request.Body()))
		if err != nil {
			return nil, err
		}
		for name, submittedValues := range parsed {
			if len(submittedValues) > 0 {
				values[name] = submittedValues[0]
			}
		}
	case "multipart/form-data":
		boundary := parameters["boundary"]
		if boundary == "" {
			return nil, errors.New("multipart form boundary is missing")
		}
		form, err := multipart.NewReader(bytes.NewReader(request.Body()), boundary).ReadForm(32 << 20)
		if err != nil {
			return nil, err
		}
		defer form.RemoveAll()
		for name, submittedValues := range form.Value {
			if len(submittedValues) > 0 {
				values[name] = submittedValues[0]
			}
		}
	default:
		return nil, fmt.Errorf("unsupported form content type: %s", mediaType)
	}
	return values, nil
}

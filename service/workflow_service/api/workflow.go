package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/sqlc-dev/pqtype"

	rdsDbLib "github.com/sugerio/workflow-service-trial/rds-db/lib"
	"github.com/sugerio/workflow-service-trial/service/workflow_service/core"
	"github.com/sugerio/workflow-service-trial/service/workflow_service/temporal"
	"github.com/sugerio/workflow-service-trial/shared/structs"
)

func (service *WorkflowService) CreateWorkflow(c *fiber.Ctx) error {
	orgId := c.Params("orgId")
	if orgId == "" {
		return HandleBadRequestErrorWithTrace(c, fmt.Errorf("orgId is empty"))
	}

	params := structs.WorkflowEntity{}
	if err := c.BodyParser(&params); err != nil {
		return HandleBadRequestErrorWithTrace(c, err)
	}
	// Validate the request params.
	if params.Name == "" {
		return HandleBadRequestErrorWithTrace(c, errors.New("name is invalid"))
	}
	// Ensure the request body has the same orgId as the path parameter.
	params.SugerOrgId = orgId
	// Ensure each node has the same orgId as the path parameter.
	for i := range params.Nodes {
		params.Nodes[i].SugerOrgId = orgId
		// Set node ID for new node if not set.
		if params.Nodes[i].ID == "" {
			params.Nodes[i].ID = uuid.NewString()
		}

		// If the node is a webhook node, set the webhook ID with a new UUID.
		// This is to ensure the webhook ID is unique.
		if core.IsWebhookNode(&params.Nodes[i]) {
			params.Nodes[i].WebhookId = uuid.NewString()
		}
	}

	// Generate new workflow ID and versionId
	params.ID = uuid.NewString()
	params.VersionId = uuid.NewString()
	// insert into execution_entity
	_, err := service.rdsDbQueries.CreateWorkflowEntity(
		c.UserContext(),
		rdsDbLib.CreateWorkflowEntityParams{
			Name:         params.Name,
			Active:       params.Active,
			Nodes:        json.RawMessage(core.JsonStr(params.Nodes)),
			Connections:  json.RawMessage(core.JsonStr(params.Connections)),
			Settings:     pqtype.NullRawMessage{json.RawMessage(core.JsonStr(params.Settings)), true},
			StaticData:   pqtype.NullRawMessage{Valid: false},
			PinData:      pqtype.NullRawMessage{json.RawMessage("{}"), true},
			VersionId:    sql.NullString{params.VersionId, true}, // new versionid.
			TriggerCount: 0,
			ID:           params.ID,
			Meta:         pqtype.NullRawMessage{Valid: false},
			SugerOrgId:   params.SugerOrgId,
		})
	if err != nil {
		return HandleInternalServerErrorWithTrace(c, err)
	}

	response := structs.GetWorkflowResponse{Data: &params}
	return c.Status(fiber.StatusOK).JSON(response)
}

func (service *WorkflowService) GetWorkflow(c *fiber.Ctx) error {
	orgId := c.Params("orgId")
	workflowId := c.Params("workflowId")
	if orgId == "" || workflowId == "" {
		return HandleBadRequestErrorWithTrace(c, fmt.Errorf("orgId or workflowId is empty"))
	}

	workflowEntity, err := core.GetWorkflowEntity(c.UserContext(), orgId, workflowId)
	if err != nil {
		return HandleInternalServerErrorWithTrace(c, err)
	}
	response := structs.GetWorkflowResponse{Data: workflowEntity}

	return c.Status(fiber.StatusOK).JSON(response)
}

// Update workflow
// reference n8n code: https://github.com/sugerio/workflow-service/blob/c1b5d949658247b19abfdb598cf4b427089cb099/packages/cli/src/workflows/workflow.service.ts#L203
func (service *WorkflowService) UpdateWorkflow(c *fiber.Ctx) error {
	orgId := c.Params("orgId")
	workflowId := c.Params("workflowId")
	if orgId == "" || workflowId == "" {
		return HandleBadRequestErrorWithTrace(c, fmt.Errorf("orgId or workflowId is empty"))
	}

	params := structs.WorkflowEntity{}
	if err := c.BodyParser(&params); err != nil {
		return HandleBadRequestErrorWithTrace(c, err)
	}

	onlyUpdateActive := false
	// If request body contains name, it means need update a new version.
	// Or the request just want to update the active status.
	if params.Name == "" {
		onlyUpdateActive = true
	}

	// Enforce the SugerOrgId and ID to be the same as the path parameters.
	params.SugerOrgId = orgId
	params.ID = workflowId
	// Ensure each node has the same orgId as the path parameter.
	for i := range params.Nodes {
		params.Nodes[i].SugerOrgId = orgId
	}

	// Just update active, handle the webhook register/unregister and return.
	if onlyUpdateActive {
		var workflowEntity *structs.WorkflowEntity
		err := service.rdsDbQueries.WithWorkflowLifecycleLock(
			c.UserContext(),
			workflowId,
			func() error {
				currentWorkflow, err := core.GetWorkflowEntity(c.UserContext(), orgId, workflowId)
				if err != nil {
					return err
				}
				webhooksMaterialized, err := core.WorkflowWebhooksAreMaterialized(
					c.UserContext(), currentWorkflow, params.Active)
				if err != nil {
					return err
				}
				scheduleMaterialized, err := temporal.ScheduleTriggerIsMaterialized(
					c.UserContext(), currentWorkflow, params.Active)
				if err != nil {
					return err
				}
				activeStateChanged := currentWorkflow.Active != params.Active
				if !activeStateChanged && webhooksMaterialized && scheduleMaterialized {
					workflowEntity = currentWorkflow
					return nil
				}

				workflowEntityForTemporal := *currentWorkflow
				workflowEntityForTemporal.Active = params.Active
				temporalLifecycleChanged := false
				if !scheduleMaterialized {
					if params.Active {
						// Set up temporal workflow for active schedule trigger.
						err = temporal.SetupTemporalWorkflow_ScheduleTrigger(
							c.UserContext(), &workflowEntityForTemporal)
					} else {
						err = temporal.TerminateTemporalWorkflow_ScheduleTrigger(
							c.UserContext(), &workflowEntityForTemporal)
					}
					if err != nil {
						return err
					}
					temporalLifecycleChanged = true
				}

				if activeStateChanged || !webhooksMaterialized {
					_, err = core.UpdateWorkflowActiveWithWebhooks(
						c.UserContext(), currentWorkflow, params.Active)
				}
				if err != nil && temporalLifecycleChanged {
					var compensationErr error
					if params.Active {
						compensationErr = temporal.TerminateTemporalWorkflow_ScheduleTrigger(
							c.UserContext(), &workflowEntityForTemporal)
					} else {
						compensationErr = temporal.SetupTemporalWorkflow_ScheduleTrigger(
							c.UserContext(), &workflowEntityForTemporal)
					}
					if compensationErr != nil {
						err = fmt.Errorf("%w; temporal compensation failed: %v", err, compensationErr)
					}
					return err
				}
				if err != nil {
					return err
				}

				workflowEntity, err = core.GetWorkflowEntity(c.UserContext(), orgId, workflowId)
				return err
			},
		)
		if err != nil {
			return HandleInternalServerErrorWithTrace(c, err)
		}
		response := structs.UpdateWorkflowResponse{Data: workflowEntity}
		return c.Status(fiber.StatusOK).JSON(response)
	}

	// Generate new versionId
	params.VersionId = uuid.NewString()
	// Set node ID for new node
	for i, node := range params.Nodes {
		if node.ID == "" {
			params.Nodes[i].ID = uuid.NewString()
		}
	}

	var workflowEntityUpdated *structs.WorkflowEntity
	err := service.rdsDbQueries.WithWorkflowLifecycleLock(
		c.UserContext(),
		workflowId,
		func() error {
			workflowEntity, err := core.GetWorkflowEntity(c.UserContext(), orgId, workflowId)
			if err != nil {
				return err
			}

			// Call hook "workflow.update" here
			if workflowEntity.Active {
				if err = temporal.TerminateTemporalWorkflow_ScheduleTrigger(
					c.UserContext(), workflowEntity); err != nil {
					return err
				}
			}
			if params.Active {
				if err = temporal.SetupTemporalWorkflow_ScheduleTrigger(
					c.UserContext(), &params); err != nil {
					return compensateTemporalWorkflowUpdate(
						c.UserContext(), workflowEntity, &params, err)
				}
			}

			// TODO: Set workflowSettings
			workflowEntityUpdated, err = core.UpdateWorkflowWithWebhooks(
				c.UserContext(),
				workflowEntity,
				rdsDbLib.UpdateWorkflowEntityParams{
					SugerOrgId:   workflowEntity.SugerOrgId,
					ID:           workflowEntity.ID,
					Name:         params.Name,
					Active:       params.Active,
					Nodes:        json.RawMessage(core.JsonStr(params.Nodes)),
					Connections:  json.RawMessage(core.JsonStr(params.Connections)),
					Settings:     pqtype.NullRawMessage{RawMessage: json.RawMessage(core.JsonStr(params.Settings)), Valid: true},
					StaticData:   pqtype.NullRawMessage{RawMessage: json.RawMessage(core.JsonStr(params.StaticData)), Valid: true},
					PinData:      pqtype.NullRawMessage{RawMessage: json.RawMessage(core.JsonStr(params.PinData)), Valid: true},
					VersionId:    sql.NullString{String: params.VersionId, Valid: true},
					TriggerCount: 0,
				})
			if err != nil {
				return compensateTemporalWorkflowUpdate(
					c.UserContext(), workflowEntity, &params, err)
			}

			// TODO: Update tagMappingRepository
			// TODO: Save version to workflowHistory
			// Call hook "workflow.afterUpdate"
			return nil
		},
	)
	if err != nil {
		return HandleInternalServerErrorWithTrace(c, err)
	}

	response := structs.UpdateWorkflowResponse{Data: workflowEntityUpdated}
	return c.Status(fiber.StatusOK).JSON(response)
}

func compensateTemporalWorkflowUpdate(
	ctx context.Context,
	previousWorkflow *structs.WorkflowEntity,
	requestedWorkflow *structs.WorkflowEntity,
	cause error,
) error {
	if requestedWorkflow.Active {
		if compensationErr := temporal.TerminateTemporalWorkflow_ScheduleTrigger(
			ctx, requestedWorkflow); compensationErr != nil {
			cause = fmt.Errorf("%w; temporal compensation failed: %v", cause, compensationErr)
		}
	}
	if previousWorkflow.Active {
		if compensationErr := temporal.SetupTemporalWorkflow_ScheduleTrigger(
			ctx, previousWorkflow); compensationErr != nil {
			cause = fmt.Errorf("%w; temporal compensation failed: %v", cause, compensationErr)
		}
	}
	return cause
}

func (service *WorkflowService) ManualRunWorkflow(ctx *fiber.Ctx) error {
	orgId := ctx.Params("orgId")
	workflowId := ctx.Params("workflowId")
	if orgId == "" || workflowId == "" {
		return HandleBadRequestErrorWithTrace(ctx, fmt.Errorf("orgId or workflowId is empty"))
	}

	params := structs.WorkflowManualRunRequest{}
	if err := ctx.BodyParser(&params); err != nil {
		return HandleBadRequestErrorWithTrace(ctx, err)
	}
	// Validate the request params.
	if params.WorkflowData == nil || params.WorkflowData.ID == "" {
		return HandleBadRequestErrorWithTrace(ctx, errors.New("workflowData is invalid"))
	}

	// Get the workflow entity
	workflowEntity, err := core.GetWorkflowEntity(ctx.UserContext(), orgId, workflowId)
	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}

	// Register test webhooks if the workflow contains webhooks.
	if core.RegisterTestWebhooksIfAny(ctx.UserContext(), workflowEntity) {
		// Start temporal workflow of UnregisterTestWebhooks
		err = temporal.StartTemporalWorkflow_UnregisterTestWebhooks(ctx.UserContext(),
			workflowEntity.SugerOrgId, workflowEntity.ID)
		if err != nil {
			// Log the error and continue to next step.
			service.Logger.Log(
				"msg", "Failed to start temporal workflow for unregister test webhook",
				"error", err,
				"orgId", workflowEntity.SugerOrgId,
				"workflowId", workflowEntity.ID,
			)
		}
		return ctx.Status(fiber.StatusOK).JSON(&structs.WorkflowManualRunResponse{
			Data: &structs.WorkflowManualRunResponseData{
				WaitingForWebhook: true,
			},
		})
	}

	additionalData, executionId, err := core.GetAdditionalDataWithHooks(
		ctx.UserContext(), structs.WorkflowExecutionMode_Manual, workflowEntity, GetContextUserId(ctx))
	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}

	workFlowExecute := core.NewWorkflowExecute(
		ctx.UserContext(), additionalData, structs.WorkflowExecutionMode_Manual)
	// execute workflow

	err = workFlowExecute.Run(ctx.UserContext(), workflowEntity)

	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}

	response := &structs.WorkflowManualRunResponse{
		Data: &structs.WorkflowManualRunResponseData{
			ExecutionId: strconv.Itoa(executionId),
		},
	}
	return ctx.Status(fiber.StatusOK).JSON(response)
}

func (service *WorkflowService) ListActiveWorkflowIds(ctx *fiber.Ctx) error {
	orgId := ctx.Params("orgId")
	if orgId == "" {
		return HandleBadRequestErrorWithTrace(ctx, fmt.Errorf("orgId is empty"))
	}

	activeWorkflowEntities, err := core.ListActiveWorkflowEntities(ctx.UserContext(), orgId)
	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}
	workflowIds := make([]string, 0, len(activeWorkflowEntities))
	for _, workflowEntity := range activeWorkflowEntities {
		workflowIds = append(workflowIds, workflowEntity.ID)
	}
	response := structs.ListActiveWorkflowIdsResponse{
		Data: workflowIds,
	}

	return ctx.Status(fiber.StatusOK).JSON(response)
}

func (service *WorkflowService) ListWorkflows(ctx *fiber.Ctx) error {
	orgId := ctx.Params("orgId")
	if orgId == "" {
		return HandleBadRequestErrorWithTrace(ctx, fmt.Errorf("orgId is empty"))
	}

	workflowEntities, err := core.ListWorkflowEntities(ctx.UserContext(), orgId)
	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}
	response := structs.ListWorkflowsResponse{
		Data:  workflowEntities,
		Count: int64(len(workflowEntities)),
	}
	return ctx.Status(fiber.StatusOK).JSON(response)
}

func (service *WorkflowService) DeleteWorkflow(ctx *fiber.Ctx) error {
	orgId := ctx.Params("orgId")
	workflowId := ctx.Params("workflowId")
	if orgId == "" || workflowId == "" {
		return HandleBadRequestErrorWithTrace(ctx, fmt.Errorf("orgId or workflowId is empty"))
	}
	var workflowDB *structs.WorkflowEntity
	err := service.rdsDbQueries.WithWorkflowLifecycleLock(
		ctx.UserContext(),
		workflowId,
		func() error {
			workflowEntity, err := core.GetWorkflowEntity(ctx.UserContext(), orgId, workflowId)
			if err != nil {
				return err
			}

			if workflowEntity.Active {
				err = temporal.TerminateTemporalWorkflow_ScheduleTrigger(ctx.UserContext(), workflowEntity)
				if err != nil {
					return err
				}
			}

			workflowDB, err = core.DeleteWorkflowWithWebhooks(ctx.UserContext(), workflowEntity)
			if err != nil {
				return compensateWorkflowDeletion(ctx.UserContext(), workflowEntity, err)
			}
			return nil
		},
	)
	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}
	response := structs.DeleteWorkflowResponse{
		Data: workflowDB != nil,
	}
	return ctx.Status(fiber.StatusOK).JSON(response)
}

func compensateWorkflowDeletion(
	ctx context.Context,
	workflowEntity *structs.WorkflowEntity,
	cause error,
) error {
	if !workflowEntity.Active {
		return cause
	}
	if compensationErr := temporal.SetupTemporalWorkflow_ScheduleTrigger(ctx, workflowEntity); compensationErr != nil {
		return fmt.Errorf("%w; temporal compensation failed: %v", cause, compensationErr)
	}
	return cause
}

func (service *WorkflowService) DeleteTestWebhook(ctx *fiber.Ctx) error {
	orgId := ctx.Params("orgId")
	workflowId := ctx.Params("workflowId")
	if orgId == "" || workflowId == "" {
		return HandleBadRequestErrorWithTrace(ctx, fmt.Errorf("orgId or workflowId is empty"))
	}
	_, err := core.GetWorkflowEntity(ctx.UserContext(), orgId, workflowId)
	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}
	err = core.UnregisterWebhook(ctx.UserContext(), workflowId, true)
	if err != nil {
		return HandleInternalServerErrorWithTrace(ctx, err)
	}

	response := structs.DeleteWorkflowTestWebhookResponse{
		Data: true,
	}
	return ctx.Status(fiber.StatusOK).JSON(response)
}

func (service *WorkflowService) RegisterRouteMethods_Workflow() {
	service.fiberApp.Get("/workflow/org/:orgId/workflow", service.ListWorkflows)
	service.fiberApp.Post("/workflow/org/:orgId/workflow", service.CreateWorkflow)
	service.fiberApp.Get("/workflow/org/:orgId/workflow/active", service.ListActiveWorkflowIds)
	service.fiberApp.Get("/workflow/org/:orgId/workflow/:workflowId", service.GetWorkflow)
	service.fiberApp.Patch("/workflow/org/:orgId/workflow/:workflowId", service.UpdateWorkflow)
	service.fiberApp.Post("/workflow/org/:orgId/workflow/:workflowId/run", service.ManualRunWorkflow)
	service.fiberApp.Delete("/workflow/org/:orgId/workflow/:workflowId", service.DeleteWorkflow)
	service.fiberApp.Delete("/workflow/org/:orgId/workflow/:workflowId/test-webhook", service.DeleteTestWebhook)
}

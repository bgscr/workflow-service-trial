package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sugerio/workflow-service-trial/shared/structs"

	"github.com/sqlc-dev/pqtype"
	rdsDbLib "github.com/sugerio/workflow-service-trial/rds-db/lib"
)

func RegisterWebhook(ctx context.Context, workflowID string, isTest bool) error {
	workflowEntity, err := GetWorkflowEntityById(ctx, workflowID)
	if err != nil {
		Errorf("Failed to get workflow entity %v", err)
		return err
	}

	if workflowEntity == nil {
		return nil
	}

	webhooks := GetWorkflowWebhooks(workflowEntity, isTest)
	if len(webhooks) == 0 {
		return nil
	}
	queries, tx, err := GetRdsDbQueries().BeginTx(ctx)
	if err != nil {
		return err
	}
	err = registerWebhook(ctx, queries, workflowEntity, isTest)
	if err != nil {
		return rollbackWebhookTransaction(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return rollbackWebhookTransaction(tx, err)
	}
	return nil
}

func registerWebhook(
	ctx context.Context,
	queries *rdsDbLib.Queries,
	workflowEntity *structs.WorkflowEntity,
	isTest bool,
) error {
	webhooks := GetWorkflowWebhooks(workflowEntity, isTest)
	if len(webhooks) == 0 {
		return nil
	}
	missingWebhooks, err := missingWebhookEntities(ctx, queries, webhooks)
	if err != nil {
		return err
	}
	for _, webhook := range missingWebhooks {
		// Create record in webhook_entity
		_, err = saveWebhookEntity(ctx, queries, &webhook)
		if err != nil {
			return fmt.Errorf(
				"webhook %s workflowId %s save failed: %w",
				webhook.WebhookId, webhook.WorkflowId, err)
		}
	}
	for _, webhook := range missingWebhooks {
		if err := CallWebhookCreateMethod(ctx, &webhook, workflowEntity); err != nil {
			return fmt.Errorf(
				"webhook %s workflowId %s create hook failed: %w",
				webhook.WebhookId, webhook.WorkflowId, err)
		}
	}

	err = updateWorkflowStaticData(ctx, queries, workflowEntity.ID, workflowEntity.StaticData)
	if err != nil {
		Errorf("Failed to update workflow static data %v", err)
		return err
	}
	return nil
}

// WorkflowWebhooksAreMaterialized reports whether every production webhook declaration
// has a corresponding persisted entity in the desired active state.
func WorkflowWebhooksAreMaterialized(
	ctx context.Context,
	workflowEntity *structs.WorkflowEntity,
	active bool,
) (bool, error) {
	webhooks := GetWorkflowWebhooks(workflowEntity, false)
	missing, err := missingWebhookEntities(ctx, GetRdsDbQueries(), webhooks)
	if err != nil {
		return false, err
	}
	if active {
		return len(missing) == 0, nil
	}
	return len(missing) == len(webhooks), nil
}

func missingWebhookEntities(
	ctx context.Context,
	queries *rdsDbLib.Queries,
	webhooks []structs.WebhookData,
) ([]structs.WebhookData, error) {
	missing := make([]structs.WebhookData, 0, len(webhooks))
	for _, webhook := range webhooks {
		entities, err := queries.ListWebhookEntities(
			ctx,
			rdsDbLib.ListWebhookEntitiesParams{
				WorkflowId: webhook.WorkflowId,
				WebhookId:  sql.NullString{String: webhook.WebhookId, Valid: true},
			},
		)
		if err != nil {
			return nil, err
		}

		materialized := false
		for _, entity := range entities {
			if entity.WebhookPath == webhook.Path &&
				entity.Method == webhook.HttpMethod &&
				entity.Node == webhook.Node {
				materialized = true
				break
			}
		}
		if !materialized {
			missing = append(missing, webhook)
		}
	}
	return missing, nil
}

func updateWorkflowStaticData(
	ctx context.Context,
	queries *rdsDbLib.Queries,
	workflowId string,
	staticData map[string]interface{},
) error {
	staticDataJson, err := json.Marshal(staticData)
	if err != nil {
		return err
	}
	_, err = queries.UpdateWorkflowEntityStaticDataByID(
		ctx,
		rdsDbLib.UpdateWorkflowEntityStaticDataByIDParams{
			ID: workflowId,
			StaticData: pqtype.NullRawMessage{
				RawMessage: staticDataJson,
				Valid:      true,
			},
		})
	if err != nil {
		return err
	}

	return nil
}

// Unregister all webhooks of the given workflow
func UnregisterWebhook(ctx context.Context, workflowID string, isTest bool) error {
	workflowEntity, err := GetWorkflowEntityById(ctx, workflowID)
	if err != nil {
		Errorf("Failed to get workflow entity %v", err)
		return err
	}

	if workflowEntity == nil {
		return nil
	}

	webhooks := GetWorkflowWebhooks(workflowEntity, isTest)
	if len(webhooks) == 0 {
		return nil
	}
	queries, tx, err := GetRdsDbQueries().BeginTx(ctx)
	if err != nil {
		return err
	}
	err = unregisterWebhook(ctx, queries, workflowEntity, isTest)
	if err != nil {
		return rollbackWebhookTransaction(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return rollbackWebhookTransaction(tx, err)
	}
	return nil
}

func unregisterWebhook(
	ctx context.Context,
	queries *rdsDbLib.Queries,
	workflowEntity *structs.WorkflowEntity,
	isTest bool,
) error {
	webhooks := GetWorkflowWebhooks(workflowEntity, isTest)
	if len(webhooks) == 0 {
		return nil
	}
	if err := deleteWebhookEntities(ctx, queries, workflowEntity, webhooks); err != nil {
		return err
	}
	for _, webhook := range webhooks {
		if err := CallWebhookDeleteMethod(ctx, &webhook, workflowEntity); err != nil {
			return fmt.Errorf(
				"webhook %s workflowId %s delete hook failed: %w",
				webhook.WebhookId, webhook.WorkflowId, err)
		}
	}

	err := updateWorkflowStaticData(ctx, queries, workflowEntity.ID, workflowEntity.StaticData)
	if err != nil {
		Errorf("Failed to update workflow static data %v", err)
		return err
	}

	return nil
}

func deleteWebhookEntities(
	ctx context.Context,
	queries *rdsDbLib.Queries,
	workflowEntity *structs.WorkflowEntity,
	webhooks []structs.WebhookData,
) error {
	for _, webhook := range webhooks {
		err := deleteWebhookEntity(ctx, queries, &webhook)
		if err != nil {
			return fmt.Errorf(
				"delete webhook entity error for workflowId %s webhookPath %s method %s: %w",
				workflowEntity.ID, webhook.Path, webhook.HttpMethod, err)
		}
	}
	return nil
}

// DeleteWorkflowWithWebhooks removes all persistent workflow lifecycle state in one transaction.
func DeleteWorkflowWithWebhooks(
	ctx context.Context,
	workflowEntity *structs.WorkflowEntity,
) (*structs.WorkflowEntity, error) {
	productionWebhooks := GetWorkflowWebhooks(workflowEntity, false)
	testWebhooks := GetWorkflowWebhooks(workflowEntity, true)
	allWebhooks := append(append([]structs.WebhookData{}, productionWebhooks...), testWebhooks...)
	externallyDeleted := make([]structs.WebhookData, 0, len(allWebhooks))
	for index := range allWebhooks {
		webhook := &allWebhooks[index]
		externallyDeleted = append(externallyDeleted, *webhook)
		if err := CallWebhookDeleteMethod(ctx, webhook, workflowEntity); err != nil {
			return nil, compensateWebhookDeletion(ctx, workflowEntity, externallyDeleted, err)
		}
	}

	queries, tx, err := GetRdsDbQueries().BeginTx(ctx)
	if err != nil {
		return nil, compensateWebhookDeletion(ctx, workflowEntity, externallyDeleted, err)
	}
	if err = deleteWebhookEntities(ctx, queries, workflowEntity, productionWebhooks); err != nil {
		return nil, compensateWebhookDeletion(
			ctx, workflowEntity, externallyDeleted, rollbackWebhookTransaction(tx, err))
	}
	if err = deleteWebhookEntities(ctx, queries, workflowEntity, testWebhooks); err != nil {
		return nil, compensateWebhookDeletion(
			ctx, workflowEntity, externallyDeleted, rollbackWebhookTransaction(tx, err))
	}
	workflowEntityDB, err := queries.DeleteWorkflowEntity(
		ctx,
		rdsDbLib.DeleteWorkflowEntityParams{
			SugerOrgId: workflowEntity.SugerOrgId,
			ID:         workflowEntity.ID,
		},
	)
	if err != nil {
		return nil, compensateWebhookDeletion(
			ctx, workflowEntity, externallyDeleted, rollbackWebhookTransaction(tx, err))
	}
	deletedWorkflow, err := structs.ToWorkflowEntity(workflowEntityDB)
	if err != nil {
		return nil, compensateWebhookDeletion(
			ctx, workflowEntity, externallyDeleted, rollbackWebhookTransaction(tx, err))
	}
	if err = tx.Commit(); err != nil {
		return nil, compensateWebhookDeletion(
			ctx, workflowEntity, externallyDeleted, rollbackWebhookTransaction(tx, err))
	}
	return &deletedWorkflow, nil
}

func compensateWebhookDeletion(
	ctx context.Context,
	workflowEntity *structs.WorkflowEntity,
	webhooks []structs.WebhookData,
	cause error,
) error {
	for index := len(webhooks) - 1; index >= 0; index-- {
		if compensationErr := CallWebhookCreateMethod(ctx, &webhooks[index], workflowEntity); compensationErr != nil {
			cause = fmt.Errorf("%w; webhook compensation failed: %v", cause, compensationErr)
		}
	}
	return cause
}

// UpdateWorkflowActiveWithWebhooks updates the workflow active state and its online webhooks in one transaction.
func UpdateWorkflowActiveWithWebhooks(
	ctx context.Context,
	workflowEntity *structs.WorkflowEntity,
	active bool,
) (*structs.WorkflowEntity, error) {
	queries, tx, err := GetRdsDbQueries().BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	workflowEntityUpdatedDB, err := queries.UpdateWorkflowEntityActive(
		ctx,
		rdsDbLib.UpdateWorkflowEntityActiveParams{
			SugerOrgId: workflowEntity.SugerOrgId,
			ID:         workflowEntity.ID,
			Active:     active,
		})
	if err != nil {
		return nil, rollbackWebhookTransaction(tx, err)
	}
	workflowEntityUpdated, err := structs.ToWorkflowEntity(workflowEntityUpdatedDB)
	if err != nil {
		return nil, rollbackWebhookTransaction(tx, err)
	}

	if active {
		err = registerWebhook(ctx, queries, &workflowEntityUpdated, false)
	} else {
		err = unregisterWebhook(ctx, queries, &workflowEntityUpdated, false)
	}
	if err != nil {
		return nil, rollbackWebhookTransaction(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, rollbackWebhookTransaction(tx, err)
	}
	return &workflowEntityUpdated, nil
}

// UpdateWorkflowWithWebhooks atomically replaces a workflow definition and its online webhook rows.
func UpdateWorkflowWithWebhooks(
	ctx context.Context,
	previousWorkflow *structs.WorkflowEntity,
	params rdsDbLib.UpdateWorkflowEntityParams,
) (*structs.WorkflowEntity, error) {
	queries, tx, err := GetRdsDbQueries().BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	if previousWorkflow.Active {
		if err = unregisterWebhook(ctx, queries, previousWorkflow, false); err != nil {
			return nil, rollbackWebhookTransaction(tx, err)
		}
	}

	workflowEntityUpdatedDB, err := queries.UpdateWorkflowEntity(ctx, params)
	if err != nil {
		return nil, rollbackWebhookTransaction(tx, err)
	}
	workflowEntityUpdated, err := structs.ToWorkflowEntity(workflowEntityUpdatedDB)
	if err != nil {
		return nil, rollbackWebhookTransaction(tx, err)
	}
	if workflowEntityUpdated.Active {
		if err = registerWebhook(ctx, queries, &workflowEntityUpdated, false); err != nil {
			return nil, rollbackWebhookTransaction(tx, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, rollbackWebhookTransaction(tx, err)
	}
	return &workflowEntityUpdated, nil
}

func rollbackWebhookTransaction(tx *sql.Tx, cause error) error {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return fmt.Errorf("%w; rollback failed: %v", cause, err)
	}
	return cause
}

// Register all webhooks of all active workflows. Regardless the orgId.
func RegisterAllWebhooks(ctx context.Context) error {
	workflowEntities, err := ListAllActiveWorkflowEntities(ctx)
	if err != nil {
		Errorf(fmt.Sprintf("Failed to list all active workflow entities: %v", err))
		return err
	}
	for _, workflowEntity := range workflowEntities {
		err := RegisterWebhook(ctx, workflowEntity.ID, false)
		if err != nil {
			Errorf(fmt.Sprintf("Failed to register webhook: %v", err))
			// Don't return error here, continue to register webhooks for other workflows.
		}
	}
	return nil
}

// Unregister all webhooks, including test-webhooks, ignoring the workflow status.
func UnregisterAllWebhooks(ctx context.Context) error {
	// Destroy webhooks by calling their delete webhook method, for example the SugerNotificationEventTrigger node
	// should unsubscribe the AWS SNS topic.
	// The workflow_entity.staticData will be updated too because some webhook data kept in it.
	// 1. Get all workflowIds in table webhook_entity
	workflowIds, err := GetRdsDbQueries().ListDistinctWorkflowIdsFromWebhookEntities(ctx)
	if err != nil {
		Errorf(fmt.Sprintf("Failed to list all webhook workflowIds: %v", err))
		return err
	}
	if len(workflowIds) == 0 {
		return nil
	}
	// 2. Unregister webhook and test-webhook of each workflow
	for _, workflowId := range workflowIds {
		err := UnregisterWebhook(ctx, workflowId, true)
		if err != nil {
			Errorf(fmt.Sprintf("Failed to unregister test webhook of %s: %v", workflowId, err))
			// Don't return error here, continue to unregister webhooks for other workflows.
		}
		err = UnregisterWebhook(ctx, workflowId, false)
		if err != nil {
			Errorf(fmt.Sprintf("Failed to unregister webhook of %s: %v", workflowId, err))
			// Don't return error here, continue to unregister webhooks for other workflows.
		}
	}
	// 3. Delete all webhook entities from db. Just to confirm again, because all entities should
	// already been deleted in previous step of UnregisterWebhook.
	return GetRdsDbQueries().DeleteAllWebhookEntities(ctx)
}

// Register test webhooks if a workflow contains webhook when manuallyRun.
// Return true if test webhooks are registered, otherwise return false.
func RegisterTestWebhooksIfAny(ctx context.Context, workflowEntity *structs.WorkflowEntity) bool {
	webhooks := GetWorkflowWebhooks(workflowEntity, true)
	if len(webhooks) == 0 {
		return false
	}

	// If only Wait node, it will return false.
	if !checkStartWebhook(webhooks) {
		return false
	}

	// Register test webhook
	err := RegisterWebhook(ctx, workflowEntity.ID, true)
	if err != nil {
		Errorf("Failed to register test webhook: %v", err)
		return false
	}

	return true
}

func checkStartWebhook(webhooks []structs.WebhookData) bool {
	for _, webhook := range webhooks {
		if !webhook.WebhookDescription.RestartWebhook {
			return true
		}
	}
	return false
}

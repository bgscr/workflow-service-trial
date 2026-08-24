package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnsSubscriptionConfirmationRecognizesJSONEnvelope(t *testing.T) {
	event, ok := parseSnsSubscriptionConfirmation(
		"text/plain; charset=UTF-8",
		[]byte(`{"Type":"SubscriptionConfirmation","Token":"token","TopicArn":"topic"}`),
	)

	require.True(t, ok)
	require.Equal(t, "token", event.Token)
	require.Equal(t, "topic", event.TopicArn)
}

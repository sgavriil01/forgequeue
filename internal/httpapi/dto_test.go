package httpapi

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
)

func testHTTPUUID() pgtype.UUID {
	return pgtype.UUID{
		Bytes: [16]byte{1, 2, 3, 4, 5},
		Valid: true,
	}
}

func TestToJobResponseIncludesRetryMetadataAndErrorMessage(t *testing.T) {
	response := toJobResponse(db.Job{
		ID:           testHTTPUUID(),
		Kind:         "test_job",
		Payload:      []byte(`{}`),
		Status:       db.JobStatusDead,
		RetryCount:   3,
		MaxRetries:   3,
		ErrorMessage: pgtype.Text{String: "handler failed", Valid: true},
	})

	if response.RetryCount != 3 {
		t.Fatalf("expected retry_count 3, got %d", response.RetryCount)
	}

	if response.MaxRetries != 3 {
		t.Fatalf("expected max_retries 3, got %d", response.MaxRetries)
	}

	if response.ErrorMessage == nil {
		t.Fatalf("expected error_message to be present")
	}

	if *response.ErrorMessage != "handler failed" {
		t.Fatalf("expected handler failed, got %s", *response.ErrorMessage)
	}
}
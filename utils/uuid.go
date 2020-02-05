
package util

import (
	"github.com/google/uuid"
)

// UUID Generate a uuid
func UUID() string {
	id := uuid.New()
	return id.String()
}
package catalog

import (
	"database/sql"
	"fmt"
	"os"
)

func LoadSecrets(db *sql.DB) error {
	keyId := os.Getenv("S3_KEY_ID")
	secret := os.Getenv("S3_SECRET")
	accountId := os.Getenv("S3_ACCOUNT_ID")

	secrets := fmt.Sprintf(`
		create secret bucket (
		    type r2,
			key_id '%s',
			secret '%s',
			account_id '%s'
		);
	`, keyId, secret, accountId)

	_, err := db.Exec(secrets)
	if err != nil {
		return fmt.Errorf("could load secrets: %w", err)
	}

	return nil
}

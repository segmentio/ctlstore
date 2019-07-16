package executive

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/pkg/errors"
	"github.com/segmentio/ctlstore/pkg/limits"
	"github.com/segmentio/ctlstore/pkg/schema"
	"github.com/segmentio/ctlstore/pkg/sqlgen"
)

var (
	ErrCookieConflict      = errors.New("Cookie conflict")
	ErrWriterNotFound      = errors.New("Writer not found")
	ErrWriterAlreadyExists = errors.New("Writer already exists with different credentials")
	ErrCookieTooLong       = fmt.Errorf("Maximum cookie length is %d bytes", limits.LimitWriterCookieSize)
)

// For access to the mutators table. Pass this a Tx that is attached
// to the right database. Throw it away when you're done!
type mutatorStore struct {
	DB        SQLDBClient
	Ctx       context.Context
	TableName string
}

func hashMutatorSecret(secret string) string {
	hasher := sha256.New()
	hasher.Write([]byte(secret))
	return hex.EncodeToString(hasher.Sum(nil))
}

// Register associates the supplied secret and writer name in the mutators table.
// If the supplied values already exist in the datbase, it is effectively a no-op.
// If the writer already exists but the secret is different, an error will be returned
// to signal this.
func (ms *mutatorStore) Register(writerName schema.WriterName, writerSecret string) error {
	secret := hashMutatorSecret(writerSecret)
	row := ms.DB.QueryRowContext(ms.Ctx,
		sqlgen.SqlSprintf("SELECT count(*) FROM $1 where writer=? and secret=?", ms.TableName),
		writerName.Name, secret)
	var count int64
	err := row.Scan(&count)
	switch {
	case err == sql.ErrNoRows:
		// this is OK, it just means that it wasn't found
	case err != nil:
		return errors.Wrap(err, "select from mutators")
	case count == 1:
		// writer already exists with this secret
		return nil
	}
	qs := sqlgen.SqlSprintf("INSERT INTO $1 (writer, secret, cookie) VALUES(?, ?, ?)", ms.TableName)
	token := []byte(tokenForWriter(writerName))
	_, err = ms.DB.ExecContext(ms.Ctx, qs, writerName.Name, secret, token)
	if err != nil && errorIsRowConflict(err) {
		return ErrWriterAlreadyExists
	}
	return err
}

func (ms *mutatorStore) Exists(writerName schema.WriterName) (bool, error) {
	qs := sqlgen.SqlSprintf("SELECT count(*) from $1 WHERE writer=?", ms.TableName)
	row := ms.DB.QueryRowContext(ms.Ctx, qs, writerName.Name)
	var count int64
	if err := row.Scan(&count); err != nil {
		return false, errors.Wrap(err, "scan writer count")
	}
	return count > 0, nil
}

func (ms *mutatorStore) Get(writerName schema.WriterName, writerSecret string) ([]byte, bool, error) {
	qs := sqlgen.SqlSprintf("SELECT cookie FROM $1 WHERE writer = ? AND secret = ? LIMIT 1", ms.TableName)
	secret := hashMutatorSecret(writerSecret)
	row := ms.DB.QueryRowContext(ms.Ctx, qs, writerName.Name, secret)
	cookieBytes := []byte{}
	err := row.Scan(&cookieBytes)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil || cookieBytes == nil {
		return nil, false, err
	}
	return cookieBytes, true, nil
}

func (ms *mutatorStore) Update(
	writerName schema.WriterName,
	writerSecret string,
	cookie []byte,
	ifCookie []byte) error {

	if len(cookie) > limits.LimitWriterCookieSize || len(ifCookie) > limits.LimitWriterCookieSize {
		return ErrCookieTooLong
	}

	// The clock field here is useful because it gives us a way to count
	// calls which do not alter the cookie to be counted below as an
	// affected row.
	qs := sqlgen.SqlSprintf("UPDATE $1 SET cookie=?, clock=clock+1 WHERE writer=? AND secret=?", ms.TableName)
	secret := hashMutatorSecret(writerSecret)
	args := []interface{}{cookie, writerName.Name, secret}
	if ifCookie != nil {
		qs += " AND cookie=?"
		args = append(args, ifCookie)
	}

	res, err := ms.DB.ExecContext(ms.Ctx, qs, args...)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		//
		// Zero rows are affected happens in the case that:
		//   1. ifCookie check failed
		//   2. Writer doesn't exist
		//
		_, found, err := ms.Get(writerName, writerSecret)
		if err != nil {
			return err
		}

		if !found {
			return ErrWriterNotFound
		}

		// Cookie check failure in UPDATE statement ends up here
		return ErrCookieConflict
	}

	return nil
}

// This is used for signing tokens, but it's not security sensitive. It just
// challenges the writer to make sure it is following the API conventions. The
// reason to use these signed tokens instead of just adding a row to the DB is
// that we want to remain RESTy and this is a GET method.
const writerTokenKey = "143zGC4aYAXdKBs9ZhcxgVcfCCv"

// Generates the token used the first time the writer initializes by signing it
// with a "secret"
func tokenForWriter(writerName schema.WriterName) string {
	h := hmac.New(sha256.New, []byte(writerTokenKey))
	sum := h.Sum([]byte(writerName.Name))
	return base64.StdEncoding.EncodeToString(sum)
}

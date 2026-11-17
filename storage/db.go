package storage

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

// DB wraps the SQLite database for chat storage.
type DB struct {
	conn *sql.DB
}

// Open creates or opens the chat database at the given path.
func Open(path string) (*DB, error) {
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Performance tuning for SQLite
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := conn.Exec(p); err != nil {
			log.Printf("[db] pragma %s: %v", p, err)
		}
	}

	db := &DB{conn: conn}
	if err := db.migrate(); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// =============================================================================
// Schema
// =============================================================================

func (db *DB) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS messages (
		id         TEXT PRIMARY KEY,
		peer_id    TEXT NOT NULL,     -- "name@host:port"
		direction  TEXT NOT NULL,     -- "sent" or "received"
		msg_type   TEXT NOT NULL,     -- "text", "image", "file", "folder"
		content    BLOB NOT NULL,     -- encrypted content (JSON)
		timestamp  INTEGER NOT NULL,  -- unix milliseconds
		file_name  TEXT,              -- original file name (if applicable)
		file_size  INTEGER,           -- file size in bytes (if applicable)
		file_path  TEXT               -- local path to saved file
	);

	CREATE TABLE IF NOT EXISTS contacts (
		id              TEXT PRIMARY KEY,  -- "name@host:port"
		name            TEXT NOT NULL,
		host            TEXT NOT NULL,
		port            INTEGER NOT NULL,
		public_key      TEXT NOT NULL,     -- hex-encoded
		fingerprint     TEXT NOT NULL,
		verified        INTEGER DEFAULT 0, -- 0=unverified, 1=verified
		last_seen       INTEGER,
		created_at      INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS settings (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);
	`
	_, err := db.conn.Exec(schema)
	return err
}

// =============================================================================
// Messages
// =============================================================================

// Message represents a stored chat message.
type Message struct {
	ID        string `json:"id"`
	PeerID    string `json:"peerId"`
	Direction string `json:"direction"` // "sent" or "received"
	MsgType   string `json:"msgType"`   // "text", "image", "file", "folder"
	Content   []byte `json:"content"`   // JSON with encrypted payload
	Timestamp int64  `json:"timestamp"` // unix milliseconds
	FileName  string `json:"fileName,omitempty"`
	FileSize  int64  `json:"fileSize,omitempty"`
	FilePath  string `json:"filePath,omitempty"`
}

// SaveMessage stores a message in the database.
func (db *DB) SaveMessage(msg *Message) error {
	_, err := db.conn.Exec(`
		INSERT OR REPLACE INTO messages (id, peer_id, direction, msg_type, content, timestamp, file_name, file_size, file_path)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, msg.ID, msg.PeerID, msg.Direction, msg.MsgType, msg.Content, msg.Timestamp, msg.FileName, msg.FileSize, msg.FilePath)
	return err
}

// GetMessages retrieves messages with a specific peer, ordered by time.
// limit=0 means no limit. offset is for pagination.
func (db *DB) GetMessages(peerID string, limit, offset int) ([]*Message, error) {
	query := `SELECT id, peer_id, direction, msg_type, content, timestamp, file_name, file_size, COALESCE(file_path,'')
		FROM messages WHERE peer_id = ? ORDER BY timestamp ASC`
	var rows *sql.Rows
	var err error
	if limit > 0 {
		query += " LIMIT ? OFFSET ?"
		rows, err = db.conn.Query(query, peerID, limit, offset)
	} else {
		rows, err = db.conn.Query(query, peerID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		m := &Message{}
		if err := rows.Scan(&m.ID, &m.PeerID, &m.Direction, &m.MsgType, &m.Content, &m.Timestamp, &m.FileName, &m.FileSize, &m.FilePath); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// GetRecentChats returns the most recent message for each peer, for the chat list.
func (db *DB) GetRecentChats() ([]*Message, error) {
	rows, err := db.conn.Query(`
		SELECT m.id, m.peer_id, m.direction, m.msg_type, m.content, m.timestamp, m.file_name, m.file_size, COALESCE(m.file_path,'')
		FROM messages m
		INNER JOIN (
			SELECT peer_id, MAX(timestamp) as max_ts
			FROM messages
			GROUP BY peer_id
		) latest ON m.peer_id = latest.peer_id AND m.timestamp = latest.max_ts
		ORDER BY m.timestamp DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		m := &Message{}
		if err := rows.Scan(&m.ID, &m.PeerID, &m.Direction, &m.MsgType, &m.Content, &m.Timestamp, &m.FileName, &m.FileSize, &m.FilePath); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// =============================================================================
// Contacts
// =============================================================================

// Contact represents a saved peer contact.
type Contact struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	PublicKey   string `json:"publicKey"`
	Fingerprint string `json:"fingerprint"`
	Verified    bool   `json:"verified"`
	LastSeen    int64  `json:"lastSeen"`
	CreatedAt   int64  `json:"createdAt"`
}

// SaveContact creates or updates a contact.
func (db *DB) SaveContact(c *Contact) error {
	verified := 0
	if c.Verified {
		verified = 1
	}
	now := time.Now().UnixMilli()
	if c.CreatedAt == 0 {
		c.CreatedAt = now
	}
	_, err := db.conn.Exec(`
		INSERT OR REPLACE INTO contacts (id, name, host, port, public_key, fingerprint, verified, last_seen, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, c.ID, c.Name, c.Host, c.Port, c.PublicKey, c.Fingerprint, verified, c.LastSeen, c.CreatedAt)
	return err
}

// GetContacts returns all saved contacts.
func (db *DB) GetContacts() ([]*Contact, error) {
	rows, err := db.conn.Query(`SELECT id, name, host, port, public_key, fingerprint, verified, last_seen, created_at FROM contacts ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var contacts []*Contact
	for rows.Next() {
		c := &Contact{}
		var verified int
		if err := rows.Scan(&c.ID, &c.Name, &c.Host, &c.Port, &c.PublicKey, &c.Fingerprint, &verified, &c.LastSeen, &c.CreatedAt); err != nil {
			return nil, err
		}
		c.Verified = verified == 1
		contacts = append(contacts, c)
	}
	return contacts, rows.Err()
}

// GetContact looks up a single contact by ID.
func (db *DB) GetContact(id string) (*Contact, error) {
	c := &Contact{}
	var verified int
	err := db.conn.QueryRow(`SELECT id, name, host, port, public_key, fingerprint, verified, last_seen, created_at FROM contacts WHERE id = ?`, id).
		Scan(&c.ID, &c.Name, &c.Host, &c.Port, &c.PublicKey, &c.Fingerprint, &verified, &c.LastSeen, &c.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.Verified = verified == 1
	return c, nil
}

// VerifyContact marks a contact's fingerprint as verified.
func (db *DB) VerifyContact(id string) error {
	_, err := db.conn.Exec(`UPDATE contacts SET verified = 1 WHERE id = ?`, id)
	return err
}

// =============================================================================
// Settings
// =============================================================================

// GetSetting retrieves a setting value.
func (db *DB) GetSetting(key string) (string, error) {
	var value string
	err := db.conn.QueryRow(`SELECT value FROM settings WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// SetSetting stores a setting value.
func (db *DB) SetSetting(key, value string) error {
	_, err := db.conn.Exec(`INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)`, key, value)
	return err
}

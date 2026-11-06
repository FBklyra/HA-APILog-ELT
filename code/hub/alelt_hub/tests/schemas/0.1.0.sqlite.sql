-- hub metadata schema of release 0.1.0 (sqlite); frozen fixture for upgrade tests (do not edit)

CREATE TABLE app_user (
	id CHAR(32) NOT NULL, 
	entra_oid VARCHAR(100), 
	email VARCHAR(320), 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (entra_oid)
);

CREATE TABLE audit_event (
	id CHAR(32) NOT NULL, 
	actor VARCHAR(320) NOT NULL, 
	role VARCHAR(30), 
	action VARCHAR(100) NOT NULL, 
	target VARCHAR(300), 
	detail JSON, 
	at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE capture_lease (
	connection_id VARCHAR(100) NOT NULL, 
	holder VARCHAR(100), 
	lease_id CHAR(32), 
	epoch BIGINT DEFAULT '0' NOT NULL, 
	ttl_s INTEGER DEFAULT '30' NOT NULL, 
	acquired_at DATETIME, 
	renewed_at DATETIME, 
	expires_at DATETIME, 
	candidates JSON, 
	PRIMARY KEY (connection_id)
);

CREATE TABLE checkpoint_mirror (
	stream VARCHAR(200) NOT NULL, 
	connection_id VARCHAR(100) NOT NULL, 
	position TEXT, 
	last_seq BIGINT DEFAULT '0' NOT NULL, 
	restart_seq BIGINT DEFAULT '0' NOT NULL, 
	open_txns INTEGER DEFAULT '0' NOT NULL, 
	oldest_open_s INTEGER DEFAULT '0' NOT NULL, 
	last_commit_ts DATETIME, 
	updated_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')), 
	PRIMARY KEY (stream)
);

CREATE TABLE config_revision (
	id INTEGER NOT NULL, 
	author VARCHAR(200) NOT NULL, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	reason TEXT, 
	body JSON, 
	PRIMARY KEY (id)
);

CREATE TABLE connection (
	id VARCHAR(100) NOT NULL, 
	engine VARCHAR(30) NOT NULL, 
	db_instance VARCHAR(200), 
	db_database VARCHAR(200), 
	legacy_connection_id VARCHAR(200), 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE diag_action (
	id VARCHAR(36) NOT NULL, 
	incident_id VARCHAR(36) NOT NULL, 
	kind VARCHAR(30) NOT NULL, 
	params JSON, 
	description TEXT, 
	safe BOOLEAN DEFAULT '0' NOT NULL, 
	rule_id VARCHAR(80), 
	status VARCHAR(20) DEFAULT 'PROPOSED' NOT NULL, 
	created_at DATETIME, 
	decided_by VARCHAR(320), 
	decided_at DATETIME, 
	reason TEXT, 
	result JSON, 
	PRIMARY KEY (id)
);

CREATE INDEX ix_diag_action_incident_id ON diag_action (incident_id);

CREATE TABLE diag_code_fix (
	id VARCHAR(36) NOT NULL, 
	incident_id VARCHAR(36) NOT NULL, 
	status VARCHAR(20) DEFAULT 'PROPOSED' NOT NULL, 
	summary TEXT NOT NULL, 
	explanation TEXT, 
	files JSON, 
	patch TEXT NOT NULL, 
	test_path TEXT, 
	validation JSON, 
	regression_case JSON, 
	provider VARCHAR(30), 
	model VARCHAR(100), 
	prompt_hash VARCHAR(64), 
	confidence FLOAT, 
	created_at DATETIME, 
	created_by VARCHAR(320) NOT NULL, 
	reviewed_by VARCHAR(320), 
	reviewed_at DATETIME, 
	review_reason TEXT, 
	exported_at DATETIME, 
	PRIMARY KEY (id)
);

CREATE INDEX ix_diag_code_fix_incident_id ON diag_code_fix (incident_id);

CREATE TABLE diag_incident (
	id VARCHAR(36) NOT NULL, 
	signature VARCHAR(64) NOT NULL, 
	kind VARCHAR(40) NOT NULL, 
	severity VARCHAR(10) NOT NULL, 
	status VARCHAR(20) DEFAULT 'OPEN' NOT NULL, 
	title TEXT NOT NULL, 
	connection_id VARCHAR(100), 
	table_fqn VARCHAR(300), 
	batch_id VARCHAR(64), 
	job_id BIGINT, 
	agent_id VARCHAR(100), 
	position TEXT, 
	error TEXT, 
	"trigger" JSON, 
	count INTEGER DEFAULT '1' NOT NULL, 
	first_seen DATETIME, 
	last_seen DATETIME, 
	rule_id VARCHAR(80), 
	root_cause TEXT, 
	explanation TEXT, 
	confidence FLOAT, 
	fix_steps JSON, 
	resolved_by VARCHAR(320), 
	resolved_at DATETIME, 
	confirmed_cause TEXT, 
	classification VARCHAR(30), 
	source_data JSON, 
	PRIMARY KEY (id)
);

CREATE INDEX ix_diag_incident_sig ON diag_incident (signature, status);

CREATE INDEX ix_diag_incident_status ON diag_incident (status, last_seen);

CREATE TABLE diag_incident_event (
	id INTEGER NOT NULL, 
	incident_id VARCHAR(36) NOT NULL, 
	at DATETIME, 
	kind VARCHAR(30) NOT NULL, 
	actor VARCHAR(320) NOT NULL, 
	detail JSON, 
	PRIMARY KEY (id)
);

CREATE INDEX ix_diag_incident_event_incident_id ON diag_incident_event (incident_id);

CREATE TABLE diag_known_issue (
	id VARCHAR(36) NOT NULL, 
	kind VARCHAR(40) NOT NULL, 
	pattern TEXT NOT NULL, 
	title TEXT NOT NULL, 
	root_cause TEXT NOT NULL, 
	fix_steps JSON, 
	source_incident VARCHAR(36), 
	created_by VARCHAR(320) NOT NULL, 
	created_at DATETIME, 
	hits INTEGER DEFAULT '0' NOT NULL, 
	category VARCHAR(30), 
	PRIMARY KEY (id)
);

CREATE TABLE diag_run (
	id VARCHAR(36) NOT NULL, 
	incident_id VARCHAR(36), 
	ref_kind VARCHAR(30) NOT NULL, 
	ref_id VARCHAR(100) NOT NULL, 
	at DATETIME, 
	actor VARCHAR(320) NOT NULL, 
	stage VARCHAR(1) NOT NULL, 
	findings JSON, 
	provider VARCHAR(30), 
	model VARCHAR(100), 
	prompt_hash VARCHAR(64), 
	answer JSON, 
	tokens INTEGER, 
	duration_ms INTEGER, 
	error TEXT, 
	PRIMARY KEY (id)
);

CREATE INDEX ix_diag_run_incident_id ON diag_run (incident_id);

CREATE TABLE diag_sensitive_sample (
	id INTEGER NOT NULL, 
	incident_id VARCHAR(36) NOT NULL, 
	column_name VARCHAR(128), 
	position TEXT, 
	"key" JSON, 
	value TEXT, 
	at DATETIME, 
	PRIMARY KEY (id)
);

CREATE INDEX ix_diag_sensitive_sample_incident_id ON diag_sensitive_sample (incident_id);

CREATE TABLE golden_capture (
	id CHAR(32) NOT NULL, 
	connection_id VARCHAR(100) NOT NULL, 
	schema_name VARCHAR(200), 
	origin VARCHAR(20) NOT NULL, 
	status VARCHAR(20) NOT NULL, 
	body JSON, 
	message TEXT, 
	captured_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE group_mapping (
	group_id VARCHAR(100) NOT NULL, 
	group_name VARCHAR(200) NOT NULL, 
	role VARCHAR(30) NOT NULL, 
	PRIMARY KEY (group_id)
);

CREATE TABLE hub_node (
	node_id VARCHAR(200) NOT NULL, 
	role VARCHAR(20) NOT NULL, 
	host VARCHAR(200), 
	version VARCHAR(50), 
	spool_url TEXT, 
	db_backend VARCHAR(20), 
	started_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	last_seen DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (node_id)
);

CREATE TABLE hub_setting (
	"key" VARCHAR(100) NOT NULL, 
	value JSON NOT NULL, 
	updated_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')), 
	PRIMARY KEY ("key")
);

CREATE TABLE import_run (
	id CHAR(32) NOT NULL, 
	started_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	finished_at DATETIME, 
	status VARCHAR(20) DEFAULT 'RUNNING' NOT NULL, 
	source_ref VARCHAR(300) DEFAULT '' NOT NULL, 
	diff JSON, 
	body JSON, 
	message TEXT, 
	started_by VARCHAR(320) NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE import_session (
	id CHAR(32) NOT NULL, 
	plugin VARCHAR(100) NOT NULL, 
	platform VARCHAR(200) DEFAULT '' NOT NULL, 
	mode VARCHAR(20) DEFAULT 'live' NOT NULL, 
	status VARCHAR(30) DEFAULT 'DISCOVERED' NOT NULL, 
	started_by VARCHAR(320) NOT NULL, 
	started_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	finished_at DATETIME, 
	files JSON, 
	inventory JSON, 
	"plan" JSON, 
	edits JSON, 
	golden JSON, 
	message TEXT, 
	requested_by VARCHAR(320), 
	approved_by VARCHAR(320), 
	applied_revision INTEGER, 
	result JSON, 
	PRIMARY KEY (id)
);

CREATE TABLE job (
	id INTEGER NOT NULL, 
	kind VARCHAR(30) NOT NULL, 
	payload JSON NOT NULL, 
	dedup_key VARCHAR(200), 
	status VARCHAR(20) DEFAULT 'QUEUED' NOT NULL, 
	attempts INTEGER DEFAULT '0' NOT NULL, 
	run_after DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	locked_by VARCHAR(100), 
	locked_at DATETIME, 
	error TEXT, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	finished_at DATETIME, 
	PRIMARY KEY (id), 
	UNIQUE (dedup_key)
);

CREATE INDEX ix_job_ready ON job (status, run_after);

CREATE TABLE pipeline_definition (
	connection_id VARCHAR(100) NOT NULL, 
	origin VARCHAR(100) NOT NULL, 
	environment VARCHAR(10) DEFAULT 'TEST' NOT NULL, 
	legacy_ref VARCHAR(300), 
	source_engine VARCHAR(30) NOT NULL, 
	destination_engine VARCHAR(30) NOT NULL, 
	source JSON, 
	destination JSON, 
	tables_doc JSON NOT NULL, 
	latency_target_s INTEGER DEFAULT '120' NOT NULL, 
	schedule JSON, 
	status VARCHAR(30) DEFAULT 'DRAFT' NOT NULL, 
	active_source VARCHAR(20) DEFAULT 'LEGACY' NOT NULL, 
	revision INTEGER, 
	created_by VARCHAR(320) NOT NULL, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	updated_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')), 
	PRIMARY KEY (connection_id)
);

CREATE TABLE pipeline_trigger (
	id CHAR(32) NOT NULL, 
	kind VARCHAR(20) NOT NULL, 
	name VARCHAR(200) NOT NULL, 
	target TEXT, 
	events JSON, 
	after_connections JSON, 
	settings JSON, 
	enabled BOOLEAN DEFAULT 1 NOT NULL, 
	legacy_ref VARCHAR(300), 
	revision INTEGER, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (kind, name)
);

CREATE TABLE recon_run (
	id CHAR(32) NOT NULL, 
	check_kind VARCHAR(50) NOT NULL, 
	started_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')), 
	finished_at DATETIME, 
	PRIMARY KEY (id)
);

CREATE TABLE snapshot_chunk (
	snapshot_id CHAR(32) NOT NULL, 
	chunk_no INTEGER NOT NULL, 
	connection_id VARCHAR(100) NOT NULL, 
	table_fqn VARCHAR(300) NOT NULL, 
	rows INTEGER NOT NULL, 
	last BOOLEAN NOT NULL, 
	header JSON NOT NULL, 
	spool_path TEXT NOT NULL, 
	status VARCHAR(20) DEFAULT 'RECEIVED' NOT NULL, 
	received_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (snapshot_id, chunk_no)
);

CREATE TABLE switch_request (
	id CHAR(32) NOT NULL, 
	scope JSON NOT NULL, 
	scope_label VARCHAR(300) NOT NULL, 
	target VARCHAR(20) NOT NULL, 
	requested_by VARCHAR(320) NOT NULL, 
	requested_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	reason TEXT NOT NULL, 
	status VARCHAR(30) DEFAULT 'PENDING_APPROVAL' NOT NULL, 
	approved_by VARCHAR(320), 
	executed_at DATETIME, 
	expires_at DATETIME NOT NULL, 
	PRIMARY KEY (id)
);

CREATE TABLE table_hold (
	connection_id VARCHAR(100) NOT NULL, 
	table_fqn VARCHAR(300) NOT NULL, 
	kind VARCHAR(20) NOT NULL, 
	state VARCHAR(30) NOT NULL, 
	held BOOLEAN DEFAULT 0 NOT NULL, 
	start_pos TEXT, 
	body JSON NOT NULL, 
	updated_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')), 
	PRIMARY KEY (connection_id, table_fqn, kind)
);

CREATE TABLE wave (
	id CHAR(32) NOT NULL, 
	name VARCHAR(200) NOT NULL, 
	table_ids JSON NOT NULL, 
	planned_at DATETIME, 
	status VARCHAR(30) DEFAULT 'PLANNED' NOT NULL, 
	created_by VARCHAR(320) NOT NULL, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (name)
);

CREATE TABLE agent (
	id VARCHAR(100) NOT NULL, 
	connection_id VARCHAR(100) NOT NULL, 
	cert_thumbprint VARCHAR(128), 
	version VARCHAR(50), 
	hostname VARCHAR(200), 
	status VARCHAR(30) DEFAULT 'REGISTERED' NOT NULL, 
	last_lri TEXT, 
	durable_pos TEXT, 
	spool_bytes BIGINT, 
	metrics JSON, 
	capabilities JSON, 
	limits JSON, 
	last_heartbeat DATETIME, 
	registered_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(connection_id) REFERENCES connection (id)
);

CREATE TABLE catalog_version (
	id INTEGER NOT NULL, 
	connection_id VARCHAR(100) NOT NULL, 
	valid_from_pos TEXT, 
	taken_at DATETIME NOT NULL, 
	sha256 VARCHAR(64) NOT NULL, 
	body JSON NOT NULL, 
	received_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (connection_id, sha256), 
	FOREIGN KEY(connection_id) REFERENCES connection (id)
);

CREATE TABLE role_assignment (
	user_id CHAR(32) NOT NULL, 
	role VARCHAR(30) NOT NULL, 
	PRIMARY KEY (user_id, role), 
	FOREIGN KEY(user_id) REFERENCES app_user (id)
);

CREATE TABLE source_table (
	id CHAR(32) NOT NULL, 
	connection_id VARCHAR(100) NOT NULL, 
	src_schema VARCHAR(128) NOT NULL, 
	src_table VARCHAR(128) NOT NULL, 
	dest_name VARCHAR(256), 
	state VARCHAR(30) DEFAULT 'DISCOVERED' NOT NULL, 
	capture_enabled BOOLEAN DEFAULT 0 NOT NULL, 
	compression VARCHAR(30), 
	engine_ids JSON, 
	PRIMARY KEY (id), 
	UNIQUE (connection_id, src_schema, src_table), 
	FOREIGN KEY(connection_id) REFERENCES connection (id)
);

CREATE TABLE agent_command (
	id CHAR(32) NOT NULL, 
	agent_id VARCHAR(100) NOT NULL, 
	name VARCHAR(30) NOT NULL, 
	args JSON, 
	issued_by VARCHAR(320) NOT NULL, 
	status VARCHAR(20) DEFAULT 'PENDING' NOT NULL, 
	result JSON, 
	issued_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	acked_at DATETIME, 
	PRIMARY KEY (id), 
	FOREIGN KEY(agent_id) REFERENCES agent (id)
);

CREATE INDEX ix_cmd_agent_status ON agent_command (agent_id, status);

CREATE TABLE batch (
	id CHAR(32) NOT NULL, 
	seq BIGINT NOT NULL, 
	connection_id VARCHAR(100) NOT NULL, 
	agent_id VARCHAR(100) NOT NULL, 
	engine VARCHAR(30) NOT NULL, 
	from_lri TEXT NOT NULL, 
	to_lri TEXT NOT NULL, 
	next_lri TEXT, 
	rec_count INTEGER NOT NULL, 
	raw_bytes BIGINT NOT NULL, 
	sha256 VARCHAR(64) NOT NULL, 
	codec VARCHAR(10) NOT NULL, 
	spool_path TEXT NOT NULL, 
	catalog_version_id INTEGER, 
	status VARCHAR(20) DEFAULT 'RECEIVED' NOT NULL, 
	events INTEGER, 
	error TEXT, 
	received_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	updated_at DATETIME, 
	PRIMARY KEY (id), 
	UNIQUE (seq), 
	FOREIGN KEY(connection_id) REFERENCES connection (id), 
	FOREIGN KEY(catalog_version_id) REFERENCES catalog_version (id)
);

CREATE INDEX ix_batch_conn_seq ON batch (connection_id, seq);

CREATE TABLE batch_table_stat (
	batch_id VARCHAR(64) NOT NULL, 
	table_fqn VARCHAR(300) NOT NULL, 
	table_id CHAR(32), 
	inserts INTEGER DEFAULT '0' NOT NULL, 
	updates INTEGER DEFAULT '0' NOT NULL, 
	deletes INTEGER DEFAULT '0' NOT NULL, 
	duration_ms INTEGER, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (batch_id, table_fqn), 
	FOREIGN KEY(table_id) REFERENCES source_table (id)
);

CREATE TABLE quarantine_event (
	id CHAR(32) NOT NULL, 
	connection_id VARCHAR(100), 
	batch_id CHAR(32), 
	table_id CHAR(32), 
	table_fqn VARCHAR(300), 
	lri TEXT, 
	reason TEXT NOT NULL, 
	raw_ref JSON, 
	raw_hex TEXT, 
	status VARCHAR(20) DEFAULT 'OPEN' NOT NULL, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(table_id) REFERENCES source_table (id)
);

CREATE TABLE recon_result (
	id CHAR(32) NOT NULL, 
	run_id CHAR(32), 
	table_id CHAR(32), 
	outcome VARCHAR(20) NOT NULL, 
	detail TEXT, 
	drill JSON, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(run_id) REFERENCES recon_run (id), 
	FOREIGN KEY(table_id) REFERENCES source_table (id)
);

CREATE TABLE schema_change_event (
	id CHAR(32) NOT NULL, 
	table_id CHAR(32), 
	kind VARCHAR(50) NOT NULL, 
	status VARCHAR(30) DEFAULT 'PENDING' NOT NULL, 
	detail JSON, 
	position TEXT, 
	policy VARCHAR(30) DEFAULT 'REQUIRE_APPROVAL' NOT NULL, 
	created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')) NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(table_id) REFERENCES source_table (id)
);

CREATE TABLE signoff (
	id CHAR(32) NOT NULL, 
	table_id CHAR(32), 
	user_id CHAR(32), 
	criterion VARCHAR(50) NOT NULL, 
	signed_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')), 
	user_name VARCHAR(320), 
	comment TEXT, 
	PRIMARY KEY (id), 
	FOREIGN KEY(table_id) REFERENCES source_table (id), 
	FOREIGN KEY(user_id) REFERENCES app_user (id)
);

CREATE TABLE source_column (
	id CHAR(32) NOT NULL, 
	table_id CHAR(32) NOT NULL, 
	name VARCHAR(128) NOT NULL, 
	engine_type VARCHAR(64) NOT NULL, 
	canonical_type VARCHAR(30) NOT NULL, 
	ordinal INTEGER NOT NULL, 
	length INTEGER, 
	scale INTEGER, 
	nullable BOOLEAN, 
	key_seq INTEGER, 
	codepage INTEGER, 
	PRIMARY KEY (id), 
	UNIQUE (table_id, name), 
	FOREIGN KEY(table_id) REFERENCES source_table (id) ON DELETE CASCADE
);

CREATE TABLE switch_state (
	table_id CHAR(32) NOT NULL, 
	active_source VARCHAR(20) DEFAULT 'LEGACY' NOT NULL, 
	switched_at DATETIME, 
	PRIMARY KEY (table_id), 
	FOREIGN KEY(table_id) REFERENCES source_table (id)
);

CREATE TABLE table_config (
	id CHAR(32) NOT NULL, 
	table_id CHAR(32) NOT NULL, 
	enabled BOOLEAN DEFAULT 1 NOT NULL, 
	sync_mode VARCHAR(30) DEFAULT 'SOFT_DELETE' NOT NULL, 
	revision INTEGER, 
	PRIMARY KEY (id), 
	UNIQUE (table_id), 
	FOREIGN KEY(table_id) REFERENCES source_table (id) ON DELETE CASCADE, 
	FOREIGN KEY(revision) REFERENCES config_revision (id)
);

CREATE TABLE column_config (
	id CHAR(32) NOT NULL, 
	column_id CHAR(32) NOT NULL, 
	enabled BOOLEAN DEFAULT 1 NOT NULL, 
	hashed BOOLEAN DEFAULT 0 NOT NULL, 
	revision INTEGER, 
	PRIMARY KEY (id), 
	UNIQUE (column_id), 
	FOREIGN KEY(column_id) REFERENCES source_column (id) ON DELETE CASCADE, 
	FOREIGN KEY(revision) REFERENCES config_revision (id)
);

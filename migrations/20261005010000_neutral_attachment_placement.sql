-- Compatibility metadata only. Resource/session/operation state belongs to the neutral service.
CREATE TABLE af_attachment_placement (
  workspace_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL UNIQUE CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
  epoch BIGINT NOT NULL CHECK (epoch > 0),
  owner TEXT NOT NULL CHECK (owner IN ('legacy','neutral')),
  phase TEXT NOT NULL CHECK (phase IN ('ACTIVE','FROZEN','BACKFILL','CLOSING','CLOSED')),
  changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE af_attachment_binding (
  workspace_id UUID NOT NULL,
  object_key TEXT NOT NULL,
  metadata_key TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  current BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (workspace_id, resource_id)
);
CREATE UNIQUE INDEX af_attachment_current_object ON af_attachment_binding(workspace_id,object_key) WHERE current;
CREATE UNIQUE INDEX af_attachment_current_metadata ON af_attachment_binding(workspace_id,metadata_key) WHERE current;

CREATE FUNCTION guard_neutral_attachment_metadata() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE w UUID;
BEGIN
  IF TG_OP = 'DELETE' THEN w := OLD.workspace_id; ELSE w := NEW.workspace_id; END IF;
  IF EXISTS(SELECT 1 FROM af_attachment_placement WHERE workspace_id=w AND owner='neutral')
     AND NOT (EXISTS(SELECT 1 FROM af_attachment_placement WHERE workspace_id=w AND phase='BACKFILL')
              AND COALESCE(current_setting('appflowy.attachment_backfill',true)=w::TEXT,FALSE)) THEN
    RAISE EXCEPTION 'attachment directory is owned by neutral service' USING ERRCODE='55000';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER guard_neutral_attachment_metadata BEFORE INSERT OR UPDATE OR DELETE ON af_blob_metadata
FOR EACH ROW EXECUTE FUNCTION guard_neutral_attachment_metadata();

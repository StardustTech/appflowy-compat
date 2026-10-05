//! AppFlowy edge mapping for the independently licensed HTTP attachment contract.
//! This module remains part of the AGPL compatibility implementation.
use crate::file::BlobKey;
use crate::pg_row::AFBlobMetadataRow;
use app_error::AppError;
use chrono::{DateTime, Utc};
use database_entity::file_dto::{
  CompleteUploadRequest, CreateUploadRequest, CreateUploadResponse, UploadPartData,
  UploadPartResponse,
};
use reqwest::{Client, Method, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Connection, FromRow, PgConnection, PgPool};
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

fn unavailable() -> AppError {
  AppError::Internal(anyhow::anyhow!(
    "Neutral attachment unavailable; retry the same request"
  ))
}
fn invalid(message: &str) -> AppError {
  AppError::InvalidRequest(message.to_owned())
}
pub fn digest(bytes: &[u8]) -> String {
  format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone, FromRow)]
pub struct Placement {
  pub workspace_id: Uuid,
  pub tenant_id: String,
  pub epoch: i64,
  pub owner: String,
  pub phase: String,
}
// Placement barriers use bounded dedicated connections. Holding a business-pool
// connection while waiting for another one can exhaust that pool under load.
static BARRIERS: Semaphore = Semaphore::const_new(8);
async fn barrier(pool: &PgPool) -> Result<(SemaphorePermit<'static>, PgConnection), AppError> {
  let permit = tokio::time::timeout(Duration::from_secs(20), BARRIERS.acquire())
    .await
    .map_err(|_| unavailable())?
    .map_err(|_| unavailable())?;
  let mut conn = tokio::time::timeout(
    Duration::from_secs(5),
    PgConnection::connect_with(&pool.connect_options()),
  )
  .await
  .map_err(|_| unavailable())??;
  sqlx::query("BEGIN").execute(&mut conn).await?;
  sqlx::query("SET LOCAL lock_timeout='20s'")
    .execute(&mut conn)
    .await?;
  Ok((permit, conn))
}
// Dropping the dedicated connection rolls back and releases its transaction locks.
pub struct Route {
  pub placement: Option<Placement>,
  guard: PgConnection,
  _permit: SemaphorePermit<'static>,
}
impl Route {
  pub async fn load(pool: &PgPool, ws: &Uuid, write: bool) -> Result<Self, AppError> {
    let (permit, mut guard) = barrier(pool).await?;
    sqlx::query("SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 8041))")
      .bind(ws.to_string())
      .execute(&mut guard)
      .await?;
    let placement=sqlx::query_as::<_,Placement>("SELECT workspace_id,tenant_id,epoch,owner,phase FROM af_attachment_placement WHERE workspace_id=$1").bind(ws).fetch_optional(&mut guard).await?;
    if let Some(p) = &placement {
      if p.phase != "ACTIVE" && !(p.phase == "FROZEN" && !write) {
        return Err(invalid("Attachment placement is frozen or closing"));
      }
    }
    Ok(Self {
      placement,
      guard,
      _permit: permit,
    })
  }
  pub fn neutral(&self) -> Option<&Placement> {
    self.placement.as_ref().filter(|p| p.owner == "neutral")
  }
  pub async fn lock_key(&mut self, key: &str) -> Result<(), AppError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 8042))")
      .bind(key)
      .execute(&mut self.guard)
      .await?;
    Ok(())
  }
}

#[derive(FromRow)]
struct Binding {
  resource_id: String,
  metadata_key: String,
}
#[derive(Deserialize)]
struct Resource {
  resource_id: String,
  media_type: String,
  byte_length: u64,
  modified_at: DateTime<Utc>,
}
impl Placement {
  fn url(&self, path: &str) -> Result<String, AppError> {
    let base = std::env::var("APPFLOWY_NEUTRAL_ATTACHMENT_URL").map_err(|_| unavailable())?;
    Ok(format!(
      "{}/v0/tenants/{}{}",
      base.trim_end_matches('/'),
      self.tenant_id,
      path
    ))
  }
  async fn request(
    &self,
    method: Method,
    path: &str,
    body: Option<Value>,
    bytes: Option<(Vec<u8>, String)>,
    key: Option<&str>,
  ) -> Result<Response, AppError> {
    let token = std::env::var("APPFLOWY_NEUTRAL_ATTACHMENT_TOKEN").map_err(|_| unavailable())?;
    let client = Client::builder()
      .no_proxy()
      .redirect(reqwest::redirect::Policy::none())
      .timeout(Duration::from_secs(20))
      .build()
      .map_err(|_| unavailable())?;
    let mut req = client
      .request(method.clone(), self.url(path)?)
      .bearer_auth(token)
      .header("X-Tenant-Epoch", self.epoch)
      .header("X-Contract-Version", "0.2.0")
      .header("X-Required-Capabilities", "server-measured-content");
    if let Some(k) = key {
      req = req.header("Idempotency-Key", k);
    }
    if let Some(j) = body {
      req = req.json(&j);
    } else if let Some((b, media)) = bytes {
      req = req
        .header("Content-Type", media)
        .header("X-Content-SHA256", digest(&b))
        .body(b);
    } else if method != Method::GET {
      req = req.body(Vec::new()).header("Content-Length", 0);
    }
    let resp = req.send().await.map_err(|_| unavailable())?;
    if resp.status().is_success() {
      return Ok(resp);
    }
    match resp.status().as_u16() {
      404 => Err(AppError::RecordNotFound(
        "Attachment resource or session not found".into(),
      )),
      400 | 409 | 411 | 413 | 415 | 422 => Err(invalid(
        "Attachment request conflicts with resource/session state or declared limits",
      )),
      _ => Err(unavailable()),
    }
  }
  async fn json(
    &self,
    method: Method,
    path: &str,
    body: Option<Value>,
    key: Option<&str>,
  ) -> Result<Value, AppError> {
    self
      .request(method, path, body, None, key)
      .await?
      .json()
      .await
      .map_err(|_| unavailable())
  }
  async fn binding(
    &self,
    pool: &PgPool,
    key: &impl BlobKey,
    create: bool,
  ) -> Result<Binding, AppError> {
    if create {
      sqlx::query("INSERT INTO af_attachment_binding(workspace_id,object_key,metadata_key,resource_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
        .bind(self.workspace_id).bind(key.object_key()).bind(key.blob_metadata_key()).bind(Uuid::new_v4().to_string()).execute(pool).await?;
    }
    sqlx::query_as::<_,Binding>("SELECT resource_id,metadata_key FROM af_attachment_binding WHERE workspace_id=$1 AND object_key=$2 AND current")
      .bind(self.workspace_id).bind(key.object_key()).fetch_optional(pool).await?.ok_or_else(||AppError::RecordNotFound("Attachment mapping not found".into()))
  }
  pub async fn put(
    &self,
    pool: &PgPool,
    key: &impl BlobKey,
    bytes: Vec<u8>,
    media: String,
  ) -> Result<(), AppError> {
    let mut binding = self.binding(pool, key, true).await?;
    // A fresh legacy Put after an acknowledged delete is a new external generation.
    // Historical mappings are retained for export; old upload IDs remain bound to the old resource.
    let deleted = self
      .json(
        Method::GET,
        &format!("/operations/delete-{}", binding.resource_id),
        None,
        None,
      )
      .await;
    match deleted {
      Ok(_) => {
        let id = Uuid::new_v4().to_string();
        sqlx::query("WITH retired AS (UPDATE af_attachment_binding SET current=false WHERE workspace_id=$1 AND resource_id=$2 RETURNING object_key,metadata_key) INSERT INTO af_attachment_binding(workspace_id,object_key,metadata_key,resource_id) SELECT $1,object_key,metadata_key,$3 FROM retired")
          .bind(self.workspace_id).bind(&binding.resource_id).bind(&id).execute(pool).await?;
        binding.resource_id = id;
      },
      Err(e) if e.is_record_not_found() => {},
      Err(e) => return Err(e),
    }
    self
      .request(
        Method::PUT,
        &format!("/resources/{}", binding.resource_id),
        None,
        Some((bytes, media)),
        Some(&format!("put-{}", binding.resource_id)),
      )
      .await?;
    Ok(())
  }
  pub async fn begin(
    &self,
    pool: &PgPool,
    key: &impl BlobKey,
    req: CreateUploadRequest,
  ) -> Result<CreateUploadResponse, AppError> {
    let b = self.binding(pool, key, true).await?;
    let s=self.json(Method::POST,"/uploads",Some(json!({"resource_id":b.resource_id,"media_type":req.content_type,"byte_length":req.file_size,"sha256":null})),Some(&format!("begin-{}",b.resource_id))).await?;
    let id = s["upload_id"].as_str().ok_or_else(unavailable)?.to_string();
    Ok(CreateUploadResponse {
      file_id: req.file_id,
      upload_id: id,
    })
  }
  async fn session(&self, pool: &PgPool, key: &impl BlobKey, upload: &str) -> Result<(), AppError> {
    if !upload
      .chars()
      .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
      || upload.is_empty()
      || upload.len() > 64
    {
      return Err(invalid("Invalid upload identifier"));
    }
    let b = self.binding(pool, key, false).await?;
    let s = self
      .json(Method::GET, &format!("/uploads/{upload}"), None, None)
      .await?;
    if s["resource_id"].as_str() != Some(&b.resource_id) {
      return Err(invalid("Upload does not belong to this external resource"));
    }
    Ok(())
  }
  pub async fn part(
    &self,
    pool: &PgPool,
    key: &impl BlobKey,
    req: UploadPartData,
  ) -> Result<UploadPartResponse, AppError> {
    self.session(pool, key, &req.upload_id).await?;
    let hash = digest(&req.body);
    let op = format!("part-{}-{}", req.upload_id, req.part_number);
    self
      .request(
        Method::PUT,
        &format!("/uploads/{}/parts/{}", req.upload_id, req.part_number),
        None,
        Some((req.body, "application/octet-stream".into())),
        Some(&op),
      )
      .await?;
    Ok(UploadPartResponse {
      e_tag: hash,
      part_num: req.part_number,
    })
  }
  pub async fn complete(
    &self,
    pool: &PgPool,
    key: &impl BlobKey,
    req: CompleteUploadRequest,
  ) -> Result<(), AppError> {
    self.session(pool, key, &req.upload_id).await?;
    let parts = req
      .parts
      .iter()
      .map(|p| json!({"number":p.part_number,"sha256":p.e_tag.trim_matches('"')}))
      .collect::<Vec<_>>();
    self
      .json(
        Method::POST,
        &format!("/uploads/{}/complete", req.upload_id),
        Some(json!({"parts":parts})),
        Some(&format!("complete-{}", req.upload_id)),
      )
      .await?;
    Ok(())
  }
  pub async fn delete(&self, pool: &PgPool, key: &impl BlobKey) -> Result<(), AppError> {
    let b = self.binding(pool, key, false).await?;
    self
      .json(
        Method::DELETE,
        &format!("/resources/{}", b.resource_id),
        None,
        Some(&format!("delete-{}", b.resource_id)),
      )
      .await?;
    Ok(())
  }
  pub async fn read(&self, pool: &PgPool, key: &impl BlobKey) -> Result<Vec<u8>, AppError> {
    let b = self.binding(pool, key, false).await?;
    let resp = self
      .request(
        Method::GET,
        &format!("/resources/{}/content", b.resource_id),
        None,
        None,
        None,
      )
      .await?;
    Ok(resp.bytes().await.map_err(|_| unavailable())?.to_vec())
  }
  fn metadata(&self, r: Resource, key: String) -> AFBlobMetadataRow {
    AFBlobMetadataRow {
      workspace_id: self.workspace_id,
      file_id: key,
      file_type: r.media_type,
      file_size: r.byte_length as i64,
      modified_at: r.modified_at,
      status: 0,
      source: 0,
      source_metadata: json!({}),
    }
  }
  pub async fn meta(&self, pool: &PgPool, key: &str) -> Result<AFBlobMetadataRow, AppError> {
    let b=sqlx::query_as::<_,Binding>("SELECT resource_id,metadata_key FROM af_attachment_binding WHERE workspace_id=$1 AND metadata_key=$2 AND current").bind(self.workspace_id).bind(key).fetch_optional(pool).await?.ok_or_else(||AppError::RecordNotFound("Attachment mapping not found".into()))?;
    let r = self
      .json(
        Method::GET,
        &format!("/resources/{}", b.resource_id),
        None,
        None,
      )
      .await?;
    Ok(self.metadata(
      serde_json::from_value(r).map_err(|_| unavailable())?,
      b.metadata_key,
    ))
  }
  pub async fn list(&self, pool: &PgPool) -> Result<Vec<AFBlobMetadataRow>, AppError> {
    let mut out = Vec::new();
    let mut path = "/resources?limit=100".to_string();
    loop {
      let page = self.json(Method::GET, &path, None, None).await?;
      for item in page["items"].as_array().ok_or_else(unavailable)? {
        let r: Resource = serde_json::from_value(item.clone()).map_err(|_| unavailable())?;
        let b:Option<(String,)>=sqlx::query_as("SELECT metadata_key FROM af_attachment_binding WHERE workspace_id=$1 AND resource_id=$2 AND current").bind(self.workspace_id).bind(&r.resource_id).fetch_optional(pool).await?;
        out.push(self.metadata(r, b.ok_or_else(unavailable)?.0));
      }
      match page["next_after"].as_str() {
        Some(next) => path = format!("/resources?limit=100&after={next}"),
        None => break,
      }
    }
    Ok(out)
  }
  pub async fn usage(&self) -> Result<u64, AppError> {
    self.json(Method::GET, "/usage", None, None).await?["ready_bytes"]
      .as_u64()
      .ok_or_else(unavailable)
  }
}

pub async fn close_workspace(pool: &PgPool, ws: &Uuid) -> Result<(), AppError> {
  let (_permit, mut tx) = barrier(pool).await?;
  sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,8041))")
    .bind(ws.to_string())
    .execute(&mut tx)
    .await?;
  let p=sqlx::query_as::<_,Placement>("UPDATE af_attachment_placement SET phase='CLOSING',changed_at=NOW() WHERE workspace_id=$1 AND owner='neutral' AND phase<>'CLOSED' RETURNING workspace_id,tenant_id,epoch,owner,phase").bind(ws).fetch_optional(&mut tx).await?;
  sqlx::query("COMMIT").execute(&mut tx).await?;
  if let Some(p) = p {
    p.json(Method::POST, "/close", None, Some("close-tenant"))
      .await?;
    sqlx::query("UPDATE af_attachment_placement SET phase='CLOSED',changed_at=NOW() WHERE workspace_id=$1 AND epoch=$2 AND owner='neutral'").bind(ws).bind(p.epoch).execute(pool).await?;
  }
  Ok(())
}
pub fn start_reconciler(pool: PgPool) {
  tokio::spawn(async move {
    loop {
      tokio::time::sleep(Duration::from_secs(3)).await;
      let rows:Result<Vec<(Uuid,)>,_>=sqlx::query_as("SELECT p.workspace_id FROM af_attachment_placement p LEFT JOIN af_workspace w ON p.workspace_id=w.workspace_id WHERE p.owner='neutral' AND p.phase<>'CLOSED' AND (p.phase='CLOSING' OR w.workspace_id IS NULL)").fetch_all(&pool).await;
      if let Ok(rows) = rows {
        for (ws,) in rows {
          if close_workspace(&pool, &ws).await.is_err() {
            tracing::warn!(workspace_id=%ws,"Attachment close remains pending");
          }
        }
      }
    }
  });
}

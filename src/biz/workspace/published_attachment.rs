use crate::state::AppState;
use app_error::AppError;
use collab::core::collab::{default_client_id, CollabOptions};
use collab::core::origin::CollabOrigin;
use collab::entity::EncodedCollab;
use collab::preclude::Collab;
use serde_json::Value;
use shared_entity::dto::publish_dto::PublishDatabaseData;
use shared_entity::dto::workspace_dto::ViewLayout;
use uuid::Uuid;

/// Public access follows a reference in the currently published snapshot of this workspace.
pub async fn is_published_attachment(
  state: &AppState,
  workspace_id: &Uuid,
  parent: Option<&str>,
  file_id: &str,
) -> Result<bool, AppError> {
  let view_ids: Vec<Uuid> = sqlx::query_scalar(
    "SELECT view_id FROM af_published_collab WHERE workspace_id = $1 AND unpublished_at IS NULL",
  )
  .bind(workspace_id)
  .fetch_all(&state.pg_pool)
  .await?;
  for view_id in view_ids {
    if let Some((metadata, data)) = state
      .published_collab_store
      .get_collab_with_view_metadata_by_view_id(&view_id)
      .await?
    {
      let target = Reference {
        workspace_id,
        parent,
        file_id,
      };
      if target.contains(&serde_json::to_value(&metadata)?, &view_id.to_string()) {
        return Ok(true);
      }
      let target = ReferenceOwned {
        workspace_id: *workspace_id,
        parent: parent.map(str::to_owned),
        file_id: file_id.to_owned(),
      };
      let found = tokio::task::spawn_blocking(move || -> Result<bool, AppError> {
        target.matches_payload(view_id, metadata.view.layout, data)
      });
      // Decoding uses the selected immutable publication payload, never the live private collab.
      if found.await?? {
        return Ok(true);
      }
    }
  }
  Ok(false)
}

struct ReferenceOwned {
  workspace_id: Uuid,
  parent: Option<String>,
  file_id: String,
}

impl ReferenceOwned {
  fn matches_payload(
    &self,
    view_id: Uuid,
    layout: ViewLayout,
    data: Vec<u8>,
  ) -> Result<bool, AppError> {
    let target = Reference {
      workspace_id: &self.workspace_id,
      parent: self.parent.as_deref(),
      file_id: &self.file_id,
    };
    match layout {
      ViewLayout::Document => target.in_collab(view_id, data),
      ViewLayout::Grid | ViewLayout::Board | ViewLayout::Calendar => {
        let payload: PublishDatabaseData = serde_json::from_slice(&data)?;
        if target.in_collab(view_id, payload.database_collab)? {
          return Ok(true);
        }
        for (id, bytes) in payload
          .database_row_collabs
          .into_iter()
          .chain(payload.database_row_document_collabs)
        {
          if target.in_collab(id, bytes)? {
            return Ok(true);
          }
        }
        Ok(false)
      },
      _ => Ok(false),
    }
  }
}

struct Reference<'a> {
  workspace_id: &'a Uuid,
  parent: Option<&'a str>,
  file_id: &'a str,
}

impl Reference<'_> {
  fn in_collab(&self, id: Uuid, data: Vec<u8>) -> Result<bool, AppError> {
    let options = CollabOptions::new(id.to_string(), default_client_id())
      .with_data_source(EncodedCollab::new_v1(Vec::<u8>::new(), data).into());
    let collab = Collab::new_with_options(CollabOrigin::Server, options)
      .map_err(|e| AppError::DecodeUpdateError(e.to_string()))?;
    Ok(self.contains(&collab.to_json_value(), &id.to_string()))
  }

  fn contains(&self, value: &Value, object_id: &str) -> bool {
    match value {
      Value::String(text) => {
        if text == self.file_id && self.parent.is_none_or(|parent| parent == object_id) {
          return true;
        }
        let expected = match self.parent {
          Some(parent) => format!(
            "/api/file_storage/{}/v1/blob/{}/{}",
            self.workspace_id, parent, self.file_id
          ),
          None => format!(
            "/api/file_storage/{}/blob/{}",
            self.workspace_id, self.file_id
          ),
        };
        let path = reqwest::Url::parse(text)
          .ok()
          .map(|url| url.path().to_owned())
          .unwrap_or_else(|| text.to_owned());
        if path.replace("%3D", "=").replace("%3d", "=") == expected {
          return true;
        }
        if let Ok(nested) = serde_json::from_str::<Value>(text) {
          if nested.is_object() || nested.is_array() {
            return self.contains(&nested, object_id);
          }
        }
        false
      },
      Value::Array(values) => values.iter().any(|v| self.contains(v, object_id)),
      Value::Object(values) => values.values().any(|v| self.contains(v, object_id)),
      _ => false,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;

  #[test]
  fn public_reference_binds_workspace_parent_and_exact_file() {
    let ws = Uuid::new_v4();
    let target = Reference {
      workspace_id: &ws,
      parent: Some("page"),
      file_id: "hash=.png",
    };
    let url = format!("https://example.invalid/api/file_storage/{ws}/v1/blob/page/hash%3D.png");
    assert!(target.contains(&json!({"data": json!({"url": url}).to_string()}), "page"));
    assert!(!target.contains(&json!("hash=.png-more"), "page"));
    assert!(!target.contains(&json!("hash=.png"), "other-page"));
    assert!(!target.contains(
      &json!(format!(
        "/api/file_storage/{}/v1/blob/page/hash=.png",
        Uuid::new_v4()
      )),
      "page"
    ));
  }
}

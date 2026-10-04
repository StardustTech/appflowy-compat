use client_api::entity::UpdateCollabWebParams;
use client_api::Client;
use collab::core::origin::CollabOrigin;
use collab::preclude::ClientID;
use collab_entity::CollabType;
use collab_folder::hierarchy_builder::NestedChildViewBuilder;
use collab_folder::{Folder, ViewLayout};
use database_entity::dto::QueryCollabParams;
use rand::random;
use std::collections::HashSet;
use std::time::Duration;
use uuid::Uuid;

/// Adds document views with the given ids to the first space of the workspace folder.
///
/// The server only publishes views that belong to the folder of the workspace, so tests that
/// publish arbitrary view ids must register them first. Returns once the server-side folder, as
/// used by the publish endpoint, contains the new views.
pub async fn add_views_to_workspace_folder(
  client: &Client,
  workspace_id: &Uuid,
  view_ids: &[Uuid],
) {
  let uid = client.get_profile().await.unwrap().uid;
  let encoded_folder = client
    .get_collab(QueryCollabParams::new(
      *workspace_id,
      CollabType::Folder,
      *workspace_id,
    ))
    .await
    .unwrap()
    .encode_collab;
  let mut folder = Folder::from_collab_doc_state(
    CollabOrigin::Empty,
    encoded_folder.into(),
    &workspace_id.to_string(),
    random::<u32>() as ClientID,
  )
  .unwrap();
  let space_id = folder
    .get_view(&workspace_id.to_string(), uid)
    .unwrap()
    .children
    .first()
    .unwrap()
    .id
    .clone();
  {
    let mut txn = folder.collab.transact_mut();
    for view_id in view_ids {
      let view = NestedChildViewBuilder::new(uid, space_id.clone())
        .with_view_id(view_id.to_string())
        .with_name(&view_id.to_string())
        .with_layout(ViewLayout::Document)
        .build()
        .view;
      folder.body.views.insert(&mut txn, view, None, uid);
    }
  }

  let folder_collab_type = CollabType::Folder;
  client
    .update_web_collab(
      workspace_id,
      workspace_id,
      UpdateCollabWebParams {
        doc_state: folder
          .encode_collab_v1(|c| folder_collab_type.validate_require_data(c))
          .unwrap()
          .doc_state
          .to_vec(),
        collab_type: folder_collab_type,
      },
    )
    .await
    .unwrap();

  wait_until_views_in_server_folder(client, workspace_id, &space_id, view_ids).await;
}

/// `GET /folder` reads the folder the same way as the publish endpoint (`get_folder` of the
/// realtime server), so the views are publishable once they are listed there.
async fn wait_until_views_in_server_folder(
  client: &Client,
  workspace_id: &Uuid,
  space_id: &str,
  view_ids: &[Uuid],
) {
  let space_id = Uuid::parse_str(space_id).unwrap();
  let expected: HashSet<Uuid> = view_ids.iter().copied().collect();
  let wait = async {
    loop {
      let space = client
        .get_workspace_folder(workspace_id, Some(1), Some(space_id))
        .await
        .unwrap();
      let listed: HashSet<Uuid> = space.children.iter().map(|v| v.view_id).collect();
      if expected.is_subset(&listed) {
        return;
      }
      tokio::time::sleep(Duration::from_millis(200)).await;
    }
  };
  tokio::time::timeout(Duration::from_secs(30), wait)
    .await
    .expect("views added to the folder are not visible to the server");
}

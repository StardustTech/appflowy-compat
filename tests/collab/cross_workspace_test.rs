//! Regression tests for cross-workspace access to collab objects.
//!
//! Each test uses two users, each one being the owner of their own workspace. A user must be able
//! to access the objects of their workspace, but must not be able to read, write, delete or
//! publish an object of the other workspace by passing its id explicitly, neither through the
//! other workspace (of which they are not a member) nor through their own workspace.

use std::time::Duration;

use app_error::ErrorCode;
use appflowy_proto::{AccessChangedReason, ClientMessage, Rid, ServerMessage, UpdateFlags};
use client_api::entity::UpdateCollabWebParams;
use client_api::error::AppResponseError;
use client_api::process_response_data;
use client_api_test::{TestClient, LOCALHOST_WS_V2};
use collab::core::collab::{default_client_id, CollabOptions, DataSource};
use collab::core::origin::CollabOrigin;
use collab::entity::EncodedCollab;
use collab::preclude::Collab;
use collab_entity::CollabType;
use database_entity::dto::{
  CollabParams, CreateCollabParams, DeleteCollabParams, QueryCollab, QueryCollabParams,
  QueryCollabResult,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::Method;
use serde_json::{json, Value};
use shared_entity::dto::workspace_dto::{CollabResponse, EmbeddedCollabQuery};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{ReadTxn, StateVector, Update};

use crate::collab::util::test_encode_collab_v1;

const COLLAB_TYPE: CollabType = CollabType::Unknown;
/// Longer than the snapshot interval of a workspace (`Workspace::SNAPSHOT_INTERVAL`, 60s), but
/// shorter than the default keepalive timeout of nginx (75s), which would otherwise close the idle
/// HTTP connection of the client while it is reused.
const SNAPSHOT_WAIT: Duration = Duration::from_secs(70);

/// A user with their own workspace and an object of this workspace holding `{"secret": <secret>}`.
struct Tenant {
  client: TestClient,
  workspace_id: Uuid,
  object_id: Uuid,
  secret: String,
}

impl Tenant {
  async fn new() -> Self {
    let client = TestClient::new_user().await;
    let workspace_id = client.workspace_id().await;
    let object_id = Uuid::new_v4();
    let secret = format!("secret-{}", Uuid::new_v4());
    client
      .api_client
      .create_collab(CreateCollabParams {
        workspace_id,
        object_id,
        encoded_collab_v1: test_encode_collab_v1(&object_id, "secret", &secret)
          .encode_to_bytes()
          .unwrap()
          .into(),
        collab_type: COLLAB_TYPE,
      })
      .await
      .unwrap();
    Self {
      client,
      workspace_id,
      object_id,
      secret,
    }
  }

  fn expected(&self) -> Value {
    json!({ "secret": self.secret })
  }

  /// Reads an object through the `v1` endpoint, without the retries of `Client::get_collab`.
  async fn read(&self, workspace_id: Uuid, object_id: Uuid) -> Result<Value, AppResponseError> {
    let url = format!(
      "{}/api/workspace/v1/{}/collab/{}?collab_type={}",
      self.client.api_client.base_url,
      workspace_id,
      object_id,
      COLLAB_TYPE.value()
    );
    let resp = self
      .client
      .api_client
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    let resp = process_response_data::<CollabResponse>(resp).await?;
    Ok(to_json(object_id, resp.encode_collab))
  }

  /// Reads an object through the deprecated endpoint that takes the ids in the body.
  async fn read_deprecated(
    &self,
    workspace_id: Uuid,
    object_id: Uuid,
  ) -> Result<Value, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/collab/{}",
      self.client.api_client.base_url, workspace_id, object_id
    );
    let resp = self
      .client
      .api_client
      .http_client_with_auth(Method::GET, &url)
      .await?
      .json(&QueryCollabParams::new(object_id, COLLAB_TYPE, workspace_id))
      .send()
      .await?;
    let resp = process_response_data::<CollabResponse>(resp).await?;
    Ok(to_json(object_id, resp.encode_collab))
  }

  async fn read_json_endpoint(
    &self,
    workspace_id: Uuid,
    object_id: Uuid,
  ) -> Result<Value, AppResponseError> {
    let url = format!(
      "{}/api/workspace/v1/{}/collab/{}/json?collab_type={}",
      self.client.api_client.base_url,
      workspace_id,
      object_id,
      COLLAB_TYPE.value()
    );
    let resp = self
      .client
      .api_client
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    let resp = process_response_data::<Value>(resp).await?;
    Ok(resp["collab"].clone())
  }

  /// The owner of the object still sees its original content.
  async fn assert_unchanged(&self) {
    assert_eq!(
      self.read(self.workspace_id, self.object_id).await.unwrap(),
      self.expected()
    );
  }
}

fn to_json(object_id: Uuid, encoded_collab: EncodedCollab) -> Value {
  let options = CollabOptions::new(object_id.to_string(), default_client_id())
    .with_data_source(encoded_collab.into());
  Collab::new_with_options(CollabOrigin::Empty, options)
    .unwrap()
    .to_json_value()
}

fn assert_denied<T>(result: Result<T, AppResponseError>) {
  let Err(err) = result else {
    panic!("cross-workspace access must be denied");
  };
  assert_eq!(err.code, ErrorCode::NotEnoughPermissions, "{:?}", err);
}

/// An update setting `{"pwned": true}` on the given object.
fn malicious_update(object_id: &Uuid) -> Vec<u8> {
  let options = CollabOptions::new(object_id.to_string(), default_client_id());
  let mut collab = Collab::new_with_options(CollabOrigin::Empty, options).unwrap();
  collab.insert("pwned", true);
  let txn = collab.transact();
  txn.encode_state_as_update_v1(&StateVector::default())
}

fn malicious_params(object_id: Uuid) -> CollabParams {
  CollabParams {
    object_id,
    encoded_collab_v1: test_encode_collab_v1(&object_id, "pwned", "true")
      .encode_to_bytes()
      .unwrap()
      .into(),
    collab_type: COLLAB_TYPE,
    updated_at: None,
  }
}

#[tokio::test]
async fn http_read_collab_of_other_workspace_test() {
  let alice = Tenant::new().await;
  let bob = Tenant::new().await;

  // Each user can read their own object.
  assert_eq!(
    bob.read(bob.workspace_id, bob.object_id).await.unwrap(),
    bob.expected()
  );
  assert_eq!(
    bob
      .read_deprecated(bob.workspace_id, bob.object_id)
      .await
      .unwrap(),
    bob.expected()
  );
  assert_eq!(
    bob
      .read_json_endpoint(bob.workspace_id, bob.object_id)
      .await
      .unwrap(),
    bob.expected()
  );
  alice.assert_unchanged().await;

  // Through the workspace of the object, of which bob is not a member.
  assert_denied(bob.read(alice.workspace_id, alice.object_id).await);
  // Through bob's own workspace.
  assert_denied(bob.read(bob.workspace_id, alice.object_id).await);
  assert_denied(bob.read_deprecated(bob.workspace_id, alice.object_id).await);
  assert_denied(
    bob
      .read_json_endpoint(bob.workspace_id, alice.object_id)
      .await,
  );
  assert_denied(
    bob
      .client
      .api_client
      .get_collab_embed_info(&bob.workspace_id, &alice.object_id)
      .await,
  );

  // Batch read: bob's object is returned, alice's object is rejected.
  let result = bob
    .client
    .api_client
    .batch_get_collab(
      &bob.workspace_id,
      vec![
        QueryCollab::new(alice.object_id, COLLAB_TYPE),
        QueryCollab::new(bob.object_id, COLLAB_TYPE),
      ],
    )
    .await
    .unwrap();
  match result.0.get(&bob.object_id).unwrap() {
    QueryCollabResult::Success { encode_collab_v1 } => {
      let encoded = EncodedCollab::decode_from_bytes(encode_collab_v1).unwrap();
      assert_eq!(to_json(bob.object_id, encoded), bob.expected());
    },
    QueryCollabResult::Failed { error } => panic!("bob can't read his object: {}", error),
  }
  assert!(
    matches!(
      result.0.get(&alice.object_id),
      Some(QueryCollabResult::Failed { .. })
    ),
    "{:?}",
    result.0.get(&alice.object_id)
  );
  // A batch read through a workspace bob is not a member of is rejected as a whole.
  assert_denied(
    bob
      .client
      .api_client
      .batch_get_collab(
        &alice.workspace_id,
        vec![QueryCollab::new(alice.object_id, COLLAB_TYPE)],
      )
      .await,
  );
  let embed_infos = bob
    .client
    .api_client
    .batch_get_collab_embed_info(
      &bob.workspace_id,
      vec![EmbeddedCollabQuery {
        collab_type: COLLAB_TYPE,
        object_id: alice.object_id,
      }],
    )
    .await
    .unwrap();
  assert!(embed_infos.is_empty());
}

#[tokio::test]
async fn http_write_collab_of_other_workspace_test() {
  let alice = Tenant::new().await;
  let mut bob = Tenant::new().await;

  // Bob can still create, update and batch create objects in his own workspace.
  let bob_new_object = Uuid::new_v4();
  bob
    .client
    .create_collab_list(&bob.workspace_id, vec![malicious_params(bob_new_object)])
    .await
    .unwrap();
  bob
    .client
    .api_client
    .update_collab(CreateCollabParams::from((
      bob.workspace_id,
      malicious_params(bob.object_id),
    )))
    .await
    .unwrap();

  // Create an object with the id of alice's object in bob's workspace.
  assert_denied(
    bob
      .client
      .api_client
      .create_collab(CreateCollabParams::from((
        bob.workspace_id,
        malicious_params(alice.object_id),
      )))
      .await,
  );
  // Overwrite alice's object, through both workspaces.
  for workspace_id in [bob.workspace_id, alice.workspace_id] {
    assert_denied(
      bob
        .client
        .api_client
        .update_collab(CreateCollabParams::from((
          workspace_id,
          malicious_params(alice.object_id),
        )))
        .await,
    );
  }
  // Batch create containing alice's object.
  assert_denied(
    bob
      .client
      .create_collab_list(
        &bob.workspace_id,
        vec![
          malicious_params(Uuid::new_v4()),
          malicious_params(alice.object_id),
        ],
      )
      .await,
  );
  // Web update of alice's object.
  assert_denied(
    bob
      .client
      .api_client
      .update_web_collab(
        &bob.workspace_id,
        &alice.object_id,
        UpdateCollabWebParams {
          doc_state: malicious_update(&alice.object_id),
          collab_type: COLLAB_TYPE,
        },
      )
      .await,
  );
  // Full sync pushes an update and returns the state of the object.
  let full_sync = bob
    .client
    .api_client
    .collab_full_sync(
      &bob.workspace_id,
      &alice.object_id,
      COLLAB_TYPE,
      malicious_update(&alice.object_id),
      StateVector::default().encode_v1(),
    )
    .await;
  assert_denied(full_sync);

  // Background writes are asynchronous: give them a chance to land before checking.
  sleep(Duration::from_secs(3)).await;
  alice.assert_unchanged().await;
  assert!(bob.read(bob.workspace_id, bob_new_object).await.is_ok());
}

#[tokio::test]
async fn http_delete_collab_of_other_workspace_test() {
  let alice = Tenant::new().await;
  let bob = Tenant::new().await;

  for workspace_id in [bob.workspace_id, alice.workspace_id] {
    assert_denied(
      bob
        .client
        .api_client
        .delete_collab(DeleteCollabParams {
          object_id: alice.object_id,
          workspace_id,
        })
        .await,
    );
  }
  alice.assert_unchanged().await;

  // Bob can delete his own object.
  bob
    .client
    .api_client
    .delete_collab(DeleteCollabParams {
      object_id: bob.object_id,
      workspace_id: bob.workspace_id,
    })
    .await
    .unwrap();
  assert!(bob.read(bob.workspace_id, bob.object_id).await.is_err());
}

/// Uses the realtime sync of the test client (websocket v1, or v2 with the `sync-v2` feature).
#[tokio::test]
async fn realtime_sync_collab_of_other_workspace_test() {
  let mut alice = Tenant::new().await;
  let mut bob = Tenant::new().await;

  // Alice has her object open, so that the server holds it in memory.
  alice
    .client
    .open_collab(alice.workspace_id, alice.object_id, COLLAB_TYPE)
    .await;
  let expected = alice.expected();
  client_api_test::assert_client_collab_include_value(
    &mut alice.client,
    &alice.object_id,
    expected,
  )
  .await
  .unwrap();

  // Bob can sync his own object.
  bob
    .client
    .open_collab(bob.workspace_id, bob.object_id, COLLAB_TYPE)
    .await;
  let expected = bob.expected();
  client_api_test::assert_client_collab_include_value(&mut bob.client, &bob.object_id, expected)
    .await
    .unwrap();

  // Bob opens alice's object through his own workspace and edits it.
  bob
    .client
    .open_collab(bob.workspace_id, alice.object_id, COLLAB_TYPE)
    .await;
  bob
    .client
    .insert_into(&alice.object_id, "pwned", true)
    .await;
  sleep(Duration::from_secs(5)).await;

  let bob_view = bob.client.get_edit_collab_json(&alice.object_id).await;
  assert!(
    bob_view.get("secret").is_none(),
    "bob received alice's data: {}",
    bob_view
  );
  alice.assert_unchanged().await;
  assert!(alice
    .client
    .get_edit_collab_json(&alice.object_id)
    .await
    .get("pwned")
    .is_none());
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect_ws_v2(
  tenant: &Tenant,
  workspace_id: Uuid,
) -> Result<WsStream, tokio_tungstenite::tungstenite::Error> {
  let url = format!(
    "{}/{}?clientId={}&deviceId={}&token={}",
    *LOCALHOST_WS_V2,
    workspace_id,
    rand::random::<u32>(),
    Uuid::new_v4(),
    tenant.client.api_client.access_token().unwrap(),
  );
  connect_async(url).await.map(|(stream, _)| stream)
}

async fn send(stream: &mut WsStream, message: ClientMessage) {
  stream
    .send(Message::Binary(message.into_bytes().unwrap().into()))
    .await
    .unwrap();
}

/// Collects the server messages about `object_id` received within `duration`.
async fn recv_for(
  stream: &mut WsStream,
  object_id: Uuid,
  duration: Duration,
) -> Vec<ServerMessage> {
  let mut messages = vec![];
  let _ = timeout(duration, async {
    while let Some(Ok(message)) = stream.next().await {
      if let Message::Binary(bytes) = message {
        let message = ServerMessage::from_bytes(&bytes).unwrap();
        let about_object = match &message {
          ServerMessage::Manifest { object_id: id, .. }
          | ServerMessage::Update { object_id: id, .. }
          | ServerMessage::AwarenessUpdate { object_id: id, .. }
          | ServerMessage::AccessChanges { object_id: id, .. } => *id == object_id,
          ServerMessage::Notification { .. } => false,
        };
        if about_object {
          messages.push(message);
        }
      }
    }
  })
  .await;
  messages
}

fn manifest(object_id: Uuid) -> ClientMessage {
  ClientMessage::Manifest {
    object_id,
    collab_type: COLLAB_TYPE,
    last_message_id: Rid::default(),
    state_vector: StateVector::default().encode_v1(),
  }
}

fn apply_updates(object_id: Uuid, messages: &[ServerMessage]) -> Value {
  let options = CollabOptions::new(object_id.to_string(), default_client_id())
    .with_data_source(DataSource::DocStateV1(vec![]));
  let mut collab = Collab::new_with_options(CollabOrigin::Empty, options).unwrap();
  {
    let mut txn = collab.transact_mut();
    for message in messages {
      if let ServerMessage::Update { update, flags, .. } = message {
        assert_eq!(*flags, UpdateFlags::Lib0v1);
        txn.apply_update(Update::decode_v1(update).unwrap()).unwrap();
      }
    }
  }
  collab.to_json_value()
}

#[tokio::test]
async fn ws_v2_connect_to_other_workspace_test() {
  let alice = Tenant::new().await;
  let bob = Tenant::new().await;

  assert!(connect_ws_v2(&bob, bob.workspace_id).await.is_ok());
  assert!(
    connect_ws_v2(&bob, alice.workspace_id).await.is_err(),
    "a non member must not open a session on the workspace"
  );
  assert!(connect_ws_v2(&alice, alice.workspace_id).await.is_ok());
}

#[tokio::test]
async fn ws_v2_read_and_write_collab_of_other_workspace_test() {
  let alice = Tenant::new().await;
  let bob = Tenant::new().await;
  let mut session = connect_ws_v2(&bob, bob.workspace_id).await.unwrap();

  // Bob gets the state of his own object.
  send(&mut session, manifest(bob.object_id)).await;
  let messages = recv_for(&mut session, bob.object_id, Duration::from_secs(5)).await;
  assert_eq!(apply_updates(bob.object_id, &messages), bob.expected());

  // Bob asks for the state of alice's object through his own session.
  send(&mut session, manifest(alice.object_id)).await;
  let messages = recv_for(&mut session, alice.object_id, Duration::from_secs(5)).await;
  assert!(
    !messages
      .iter()
      .any(|m| matches!(m, ServerMessage::Update { .. })),
    "bob received alice's data: {:?}",
    messages
  );
  assert!(
    messages.iter().any(|m| matches!(
      m,
      ServerMessage::AccessChanges {
        can_read: false,
        reason: AccessChangedReason::PermissionDenied,
        ..
      }
    )),
    "{:?}",
    messages
  );

  // Bob pushes an update to alice's object through his own session.
  send(
    &mut session,
    ClientMessage::Update {
      object_id: alice.object_id,
      collab_type: COLLAB_TYPE,
      flags: UpdateFlags::Lib0v1,
      update: malicious_update(&alice.object_id),
    },
  )
  .await;
  sleep(Duration::from_secs(3)).await;
  alice.assert_unchanged().await;

  // An accepted update would only be persisted by the periodic snapshot of bob's workspace, which
  // would overwrite the cached state of alice's object. Keep the session alive (reading answers
  // the server pings) for longer than the snapshot interval, then check again.
  let messages = recv_for(&mut session, alice.object_id, SNAPSHOT_WAIT).await;
  assert!(
    !messages
      .iter()
      .any(|m| matches!(m, ServerMessage::Update { .. })),
    "{:?}",
    messages
  );
  alice.assert_unchanged().await;
  assert_eq!(
    bob.read(bob.workspace_id, bob.object_id).await.unwrap(),
    bob.expected()
  );

  // The update was not accepted in bob's workspace either.
  send(&mut session, manifest(alice.object_id)).await;
  let messages = recv_for(&mut session, alice.object_id, Duration::from_secs(5)).await;
  assert!(
    !messages
      .iter()
      .any(|m| matches!(m, ServerMessage::Update { .. })),
    "{:?}",
    messages
  );
}

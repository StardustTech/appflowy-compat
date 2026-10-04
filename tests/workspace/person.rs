use std::collections::HashSet;

use app_error::ErrorCode;
use client_api::entity::{AFRole, PageMentionUpdate, WorkspaceMemberProfile};
use client_api_test::TestClient;
use uuid::Uuid;

#[tokio::test]
async fn workspace_mentionable_persons_crud() {
  let owner = TestClient::new_user().await;
  let guest = TestClient::new_user().await;
  let guest_name = guest.api_client.get_profile().await.unwrap().name.unwrap();
  let workspace_id = owner.workspace_id().await;
  owner
    .invite_and_accepted_workspace_member(&workspace_id, &guest, AFRole::Guest)
    .await
    .unwrap();
  let workspaces = owner.api_client.get_workspaces().await.unwrap();
  assert_eq!(workspaces.len(), 1);
  let workspace_id = workspaces[0].workspace_id;
  owner
    .api_client
    .update_workspace_member_profile(
      &workspace_id,
      &WorkspaceMemberProfile {
        name: "name override".to_string(),
        avatar_url: Some("avatar url override".to_string()),
        cover_image_url: Some("cover image url".to_string()),
        custom_image_url: Some("custom image url".to_string()),
        description: Some("description override".to_string()),
      },
    )
    .await
    .unwrap();

  let mentionable_persons = owner
    .api_client
    .list_workspace_mentionable_persons(&workspace_id)
    .await
    .unwrap()
    .persons;
  assert_eq!(mentionable_persons.len(), 2);
  let mentionable_person_names: HashSet<String> =
    mentionable_persons.iter().map(|p| p.name.clone()).collect();
  assert!(mentionable_person_names.contains("name override"));
  assert!(mentionable_person_names.contains(&guest_name));
  let person_id = mentionable_persons
    .iter()
    .find(|p| p.name == guest_name)
    .unwrap()
    .uuid;
  let mentionable_person = owner
    .api_client
    .get_workspace_mentionable_person(&workspace_id, &person_id)
    .await
    .unwrap();
  assert_eq!(mentionable_person.name, guest_name);

  let folder_view = owner
    .api_client
    .get_workspace_folder(&workspace_id, Some(2), None)
    .await
    .unwrap();
  let general_space = &folder_view
    .children
    .into_iter()
    .find(|v| v.name == "General")
    .unwrap();
  let todo = general_space
    .children
    .iter()
    .find(|v| v.name == "To-dos")
    .unwrap();
  let view_id = todo.view_id;
  owner
    .api_client
    .update_page_mention(
      &workspace_id,
      &view_id,
      &PageMentionUpdate {
        person_id,
        block_id: None,
        require_notification: false,
        view_name: "To-dos".to_string(),
      },
    )
    .await
    .unwrap();
  let mentionable_persons_with_last_mentioned_time = owner
    .api_client
    .list_workspace_mentionable_persons(&workspace_id)
    .await
    .unwrap();
  assert_eq!(
    mentionable_persons_with_last_mentioned_time.persons.len(),
    2
  );
  assert_eq!(
    mentionable_persons_with_last_mentioned_time.persons[0].uuid,
    person_id
  );
  let last_mentioned_at = mentionable_persons_with_last_mentioned_time.persons[0].last_mentioned_at;
  assert!(last_mentioned_at.is_some());

  let mentionable_persons_with_access = owner
    .api_client
    .list_page_mentionable_persons(&workspace_id, &view_id)
    .await
    .unwrap()
    .persons;
  assert_eq!(mentionable_persons_with_access.len(), 2);
  let guest_can_access = mentionable_persons_with_access
    .iter()
    .find(|p| p.person.name == guest_name)
    .unwrap()
    .can_access_page;
  assert!(!guest_can_access);
  let owner_can_access = mentionable_persons_with_access
    .iter()
    .find(|p| p.person.name == "name override")
    .unwrap()
    .can_access_page;
  assert!(owner_can_access);
}

async fn todo_view_id(client: &TestClient, workspace_id: &Uuid) -> Uuid {
  general_space_view_id(client, workspace_id, "To-dos").await
}

async fn general_space_view_id(client: &TestClient, workspace_id: &Uuid, name: &str) -> Uuid {
  let folder_view = client
    .api_client
    .get_workspace_folder(workspace_id, Some(2), None)
    .await
    .unwrap();
  let general_space = folder_view
    .children
    .into_iter()
    .find(|v| v.name == "General")
    .unwrap();
  general_space
    .children
    .iter()
    .find(|v| v.name == name)
    .unwrap()
    .view_id
}

fn mention(person_id: Uuid) -> PageMentionUpdate {
  PageMentionUpdate {
    person_id,
    block_id: None,
    require_notification: true,
    view_name: "Click here to reset your password".to_string(),
  }
}

#[tokio::test]
async fn mention_person_of_other_workspace_test() {
  let alice = TestClient::new_user().await;
  let alice_workspace_id = alice.workspace_id().await;
  let alice_uuid = alice.api_client.get_profile().await.unwrap().uuid;
  let alice_view_id = todo_view_id(&alice, &alice_workspace_id).await;
  // A document view: its collab is stored under the view id, unlike the collab of a board view.
  let alice_document_id =
    general_space_view_id(&alice, &alice_workspace_id, "Getting started").await;
  let bob = TestClient::new_user().await;
  let bob_workspace_id = bob.workspace_id().await;
  let bob_uuid = bob.api_client.get_profile().await.unwrap().uuid;
  let bob_view_id = todo_view_id(&bob, &bob_workspace_id).await;

  // Bob mentions alice, who is not a person of his workspace, in one of his pages.
  let err = bob
    .api_client
    .update_page_mention(&bob_workspace_id, &bob_view_id, &mention(alice_uuid))
    .await
    .unwrap_err();
  assert_eq!(err.code, ErrorCode::RecordNotFound, "{:?}", err);
  // Bob mentions himself in a page that doesn't exist, and in pages of alice's workspace.
  for view_id in [Uuid::new_v4(), alice_view_id] {
    let err = bob
      .api_client
      .update_page_mention(&bob_workspace_id, &view_id, &mention(bob_uuid))
      .await
      .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidFolderView, "{:?}", err);
  }
  let err = bob
    .api_client
    .update_page_mention(&bob_workspace_id, &alice_document_id, &mention(bob_uuid))
    .await
    .unwrap_err();
  assert_eq!(err.code, ErrorCode::NotEnoughPermissions, "{:?}", err);
  // Bob mentions alice through her workspace, of which he is not a member.
  let err = bob
    .api_client
    .update_page_mention(&alice_workspace_id, &alice_view_id, &mention(alice_uuid))
    .await
    .unwrap_err();
  assert_eq!(err.code, ErrorCode::NotEnoughPermissions, "{:?}", err);

  // Bob can mention himself in his own page.
  bob
    .api_client
    .update_page_mention(&bob_workspace_id, &bob_view_id, &mention(bob_uuid))
    .await
    .unwrap();
}

#[tokio::test]
async fn guest_mentionable_persons_test() {
  let owner = TestClient::new_user().await;
  let workspace_id = owner.workspace_id().await;
  let view_id = todo_view_id(&owner, &workspace_id).await;
  let owner_uuid = owner.api_client.get_profile().await.unwrap().uuid;
  let member = TestClient::new_user().await;
  let member_uuid = member.api_client.get_profile().await.unwrap().uuid;
  let guest = TestClient::new_user().await;
  let guest_uuid = guest.api_client.get_profile().await.unwrap().uuid;
  owner
    .invite_and_accepted_workspace_member(&workspace_id, &member, AFRole::Member)
    .await
    .unwrap();
  owner
    .invite_and_accepted_workspace_member(&workspace_id, &guest, AFRole::Guest)
    .await
    .unwrap();
  let all: HashSet<Uuid> = [owner_uuid, member_uuid, guest_uuid].into();
  let visible_to_guest: HashSet<Uuid> = [owner_uuid, guest_uuid].into();

  // Members see every person of the workspace.
  let persons: HashSet<Uuid> = member
    .api_client
    .list_workspace_mentionable_persons(&workspace_id)
    .await
    .unwrap()
    .persons
    .iter()
    .map(|p| p.uuid)
    .collect();
  assert_eq!(persons, all);

  // A guest only sees themselves and the owner.
  let persons: HashSet<Uuid> = guest
    .api_client
    .list_workspace_mentionable_persons(&workspace_id)
    .await
    .unwrap()
    .persons
    .iter()
    .map(|p| p.uuid)
    .collect();
  assert_eq!(persons, visible_to_guest);
  let persons: HashSet<Uuid> = guest
    .api_client
    .list_page_mentionable_persons(&workspace_id, &view_id)
    .await
    .unwrap()
    .persons
    .iter()
    .map(|p| p.person.uuid)
    .collect();
  assert_eq!(persons, visible_to_guest);
  for person_id in [owner_uuid, guest_uuid] {
    guest
      .api_client
      .get_workspace_mentionable_person(&workspace_id, &person_id)
      .await
      .unwrap();
  }
  let err = guest
    .api_client
    .get_workspace_mentionable_person(&workspace_id, &member_uuid)
    .await
    .unwrap_err();
  assert_eq!(err.code, ErrorCode::RecordNotFound, "{:?}", err);

  // A guest can't mention, and so notify, a person hidden from them.
  let err = guest
    .api_client
    .update_page_mention(&workspace_id, &view_id, &mention(member_uuid))
    .await
    .unwrap_err();
  assert_eq!(err.code, ErrorCode::RecordNotFound, "{:?}", err);
  guest
    .api_client
    .update_page_mention(&workspace_id, &view_id, &mention(owner_uuid))
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_workspace_member_profile_test() {
  let owner = TestClient::new_user().await;
  let workspace_id = owner.workspace_id().await;
  let profile = |name: &str, image: Option<&str>| WorkspaceMemberProfile {
    name: name.to_string(),
    avatar_url: image.map(str::to_string),
    cover_image_url: None,
    custom_image_url: None,
    description: None,
  };

  for invalid in [
    profile("", None),
    profile("   ", None),
    profile("name\u{202e}", None),
    profile(&"a".repeat(257), None),
    profile("name", Some("javascript:alert(document.cookie)")),
    profile("name", Some(" JavaScript:alert(1)")),
    profile("name", Some("data:text/html,<script>alert(1)</script>")),
    profile("name", Some(&format!("https://example.com/{}", "a".repeat(2048)))),
  ] {
    let err = owner
      .api_client
      .update_workspace_member_profile(&workspace_id, &invalid)
      .await
      .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidRequest, "{invalid:?}");
  }

  // An emoji, a built-in cover id or an http(s) URL remain accepted.
  for valid in [
    profile("name", Some("😀")),
    profile("name", Some("1")),
    profile("name", Some("https://example.com/avatar.png")),
  ] {
    owner
      .api_client
      .update_workspace_member_profile(&workspace_id, &valid)
      .await
      .unwrap();
  }
}

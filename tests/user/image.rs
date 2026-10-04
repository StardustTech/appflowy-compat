use app_error::ErrorCode;
use client_api::entity::UserImageAssetSource;
use client_api::error::AppResponseError;
use client_api::process_response_data;
use client_api_test::generate_unique_registered_user_client;
use reqwest::Method;

#[tokio::test]
async fn upload_and_retrieve_image_asset() {
  let (client, _) = generate_unique_registered_user_client().await;
  let asset_source = client
    .upload_user_image_asset("tests/user/asset/avatar.png")
    .await
    .unwrap();
  let file_id = asset_source.file_id;
  let person_id = client.get_profile().await.unwrap().uuid;
  client
    .get_user_image_asset(&person_id, &file_id)
    .await
    .unwrap();
}

async fn upload_raw_image_asset(
  client: &client_api::Client,
  data: &'static [u8],
  file_name: &str,
  mime: &str,
) -> Result<UserImageAssetSource, AppResponseError> {
  let part = reqwest::multipart::Part::bytes(data)
    .file_name(file_name.to_string())
    .mime_str(mime)
    .unwrap();
  let form = reqwest::multipart::Form::new().part("asset", part);
  let url = format!("{}/api/user/asset/image", client.base_url);
  let resp = client
    .http_client_with_auth(Method::POST, &url)
    .await?
    .multipart(form)
    .send()
    .await?;
  process_response_data(resp).await
}

#[tokio::test]
async fn upload_image_asset_format_test() {
  let (client, _) = generate_unique_registered_user_client().await;
  let svg: &[u8] =
    br#"<svg xmlns="http://www.w3.org/2000/svg"><script>alert(document.domain)</script></svg>"#;
  let html: &[u8] = b"<html><script>alert(document.domain)</script></html>";

  // Scriptable or unknown content is rejected, whatever the declared type and file name.
  for (data, file_name, mime) in [
    (svg, "avatar.svg", "image/svg+xml"),
    (svg, "avatar.png", "image/png"),
    (html, "avatar.png", "image/png"),
  ] {
    let err = upload_raw_image_asset(&client, data, file_name, mime)
      .await
      .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidContentType, "{:?}", err);
  }

  // A real image is stored with the type of its content, not the declared one.
  let png: &'static [u8] = include_bytes!("asset/avatar.png");
  let source = upload_raw_image_asset(&client, png, "avatar.html", "image/svg+xml")
    .await
    .unwrap();
  assert!(source.file_id.ends_with(".png"), "{}", source.file_id);
  let person_id = client.get_profile().await.unwrap().uuid;
  let url = format!(
    "{}/api/user/asset/image/person/{}/file/{}",
    client.base_url, person_id, source.file_id
  );
  let resp = client
    .http_client_with_auth(Method::GET, &url)
    .await
    .unwrap()
    .send()
    .await
    .unwrap();
  assert_eq!(resp.headers()["content-type"], "image/png");
  assert_eq!(resp.headers()["x-content-type-options"], "nosniff");
  assert_eq!(resp.bytes().await.unwrap().as_ref(), png);
}

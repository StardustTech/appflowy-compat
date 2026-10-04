use actix_multipart::form::bytes::Bytes;
use app_error::AppError;
use aws_sdk_s3::primitives::ByteStream;
use database::file::{s3_client_impl::AwsS3BucketClientImpl, BucketClient, ResponseBlob};
use database_entity::dto::UserImageAssetContent;
use uuid::Uuid;

fn user_image_asset_object_key(person_id: &Uuid, file_id: &str) -> String {
  format!("user-asset/image/{}/{}", person_id, file_id)
}

/// Raster image formats accepted for user image assets, identified by their file signature.
/// SVG is not accepted, since it can embed scripts that run when the file is opened directly.
const ALLOWED_IMAGE_FORMATS: [(&str, &str, &[u8]); 4] = [
  ("image/png", "png", b"\x89PNG\r\n\x1a\n"),
  ("image/jpeg", "jpg", b"\xff\xd8\xff"),
  ("image/gif", "gif", b"GIF8"),
  ("image/webp", "webp", b"RIFF"),
];

/// Returns the content type and the file extension of the image, based on its content only.
fn detect_image_format(data: &[u8]) -> Option<(&'static str, &'static str)> {
  ALLOWED_IMAGE_FORMATS
    .iter()
    .find(|(content_type, _, signature)| {
      data.starts_with(signature)
        && (*content_type != "image/webp" || data.get(8..12) == Some(b"WEBP".as_slice()))
    })
    .map(|(content_type, extension, _)| (*content_type, *extension))
}

pub async fn upload_user_image_asset(
  client: AwsS3BucketClientImpl,
  image_content: &Bytes,
  person_id: &Uuid,
) -> Result<String, AppError> {
  match &image_content.content_type {
    Some(content_type) if content_type.type_() == mime::IMAGE => Ok(()),
    Some(content_type) => Err(AppError::InvalidContentType(format!(
      "{} is not a valid image type",
      content_type
    ))),
    None => Err(AppError::InvalidContentType(
      "Missing mime type for image upload".to_string(),
    )),
  }?;
  // The declared content type and file name come from the client: the stored content type and
  // extension are derived from the data itself.
  let (content_type, extension) = detect_image_format(&image_content.data).ok_or_else(|| {
    AppError::InvalidContentType(
      "Unsupported image format, expected PNG, JPEG, GIF or WebP".to_string(),
    )
  })?;
  let file_id = format!("{}.{}", Uuid::new_v4(), extension);

  let object_key = user_image_asset_object_key(person_id, &file_id);
  client
    .put_blob_with_content_type(
      &object_key,
      ByteStream::from(image_content.data.to_vec()),
      content_type,
    )
    .await?;
  Ok(file_id.to_string())
}

pub async fn get_user_image_asset(
  client: AwsS3BucketClientImpl,
  person_id: &Uuid,
  file_id: String,
) -> Result<UserImageAssetContent, AppError> {
  let object_key = user_image_asset_object_key(person_id, &file_id);
  let resp = client.get_blob(&object_key).await?;
  let content_type = resp.content_type().ok_or(AppError::InvalidContentType(
    "Missing content type for avatar".to_string(),
  ))?;
  // Assets uploaded before the format check may have any content type: never serve them as
  // something a browser could render as a document.
  let content_type = if ALLOWED_IMAGE_FORMATS
    .iter()
    .any(|(allowed, _, _)| *allowed == content_type)
  {
    content_type.to_string()
  } else {
    "application/octet-stream".to_string()
  };
  Ok(UserImageAssetContent {
    data: resp.to_blob(),
    content_type,
  })
}

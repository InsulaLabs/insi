# Objects Plugin API Documentation

## Overview

The Objects Plugin provides a simple binary object storage solution. It allows authenticated users to upload files, which are then stored on the server with associated metadata. These files can later be downloaded or their hashes retrieved using a unique `objectID`.

The plugin prefixes its routes with `/objects`. So, an endpoint like `/upload` will be accessible at `/objects/upload`.

### Object ID

When a file is uploaded, it is assigned a unique identifier specific to that file, referred to as `objectID` in this API documentation (internally, this is an `objectFileUUID`). This `objectID` is returned to the client upon successful upload and is used by the client to subsequently download the file or retrieve its hash.

The server internally combines this client-facing `objectID` with the authenticated user's UUID (derived from their token) to determine the actual storage path and enforce access control. The user's UUID is not exposed to the client.

## Authentication

All endpoints require a valid authentication token. The method of providing this token (e.g., `Authorization: Bearer <token>` header, cookie) depends on the main application's authentication mechanism, which is handled by the `RT_ValidateAuthToken` interface function. Ensure the client sends the token as expected by the application.

## Endpoints

### 1. Upload File

Uploads a binary file to the object storage.

-   **Method:** `POST`
-   **Path:** `/objects/upload`
-   **Description:** Accepts a multipart/form-data request containing the file to be uploaded. The file is stored, and metadata (including original filename, size, upload date, content type, user UUID, and the object's own UUID) is saved.
-   **Authentication:** Required.

#### Request

-   **Headers:**
    -   `Content-Type: multipart/form-data; boundary=<BOUNDARY_STRING>`
    -   `Authorization: Bearer <YOUR_AUTH_TOKEN>` (or other configured auth mechanism)
-   **Body:** `multipart/form-data`
    -   **`file`**: The binary file being uploaded. This is a form field of type `file`.

#### Responses

-   **`200 OK`**
    -   **Content-Type:** `application/json`
    -   **Body:**
        ```json
        {
          "status": "OK",
          "objectID": "<OBJECT_FILE_UUID>"
        }
        ```
        -   `objectID`: The unique identifier for the uploaded object (this is the file's specific UUID). Use this ID to download the file or get its hash.

-   **`400 Bad Request`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** Error message (e.g., "Error parsing form data: ...", "Error retrieving file: ...").
    -   *Reason:* Malformed request, missing file in form-data, or upload size exceeds the server limit (currently 32MB).

-   **`401 Unauthorized`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Unauthorized" or "Unauthorized or Invalid Token".
    -   *Reason:* Authentication token is missing, invalid, or expired.

-   **`403 Forbidden`**
    -   *Note: This status is generally not returned by upload unless there's a pre-upload check that fails due to permissions, which is not the case here. Token validation handles auth.*

-   **`405 Method Not Allowed`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Method not allowed".
    -   *Reason:* An HTTP method other than `POST` was used.

-   **`500 Internal Server Error`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** Error message (e.g., "Could not create storage for file", "Error saving file", "Error processing file", "Error saving file metadata").
    -   *Reason:* Server-side error during file processing, storage, or metadata creation.

#### Example `curl`

```bash
curl -X POST \
  -H "Authorization: Bearer <YOUR_AUTH_TOKEN>" \
  -F "file=@/path/to/your/example.jpg" \
  http://<your-server-address>/objects/upload
```

---

### 2. Download File

Downloads a previously uploaded binary file.

-   **Method:** `GET`
-   **Path:** `/objects/download`
-   **Description:** Retrieves a file using its `objectID` (the file's specific UUID).
-   **Authentication:** Required.

#### Request

-   **Headers:**
    -   `Authorization: Bearer <YOUR_AUTH_TOKEN>` (or other configured auth mechanism)
-   **Query Parameters:**
    -   **`id`** (string, required): The `objectID` (file's specific UUID) of the file to download.

#### Responses

-   **`200 OK`**
    -   **Headers:**
        -   `Content-Type`: The original content type of the uploaded file (e.g., `image/jpeg`, `application/pdf`). Defaults to `application/octet-stream` if the original type isn't available in metadata.
        -   `Content-Disposition: attachment; filename="<ORIGINAL_FILENAME>"`: Suggests the browser to download the file with its original name.
        -   Other headers like `Content-Length`, `ETag`, `Last-Modified` may be set by `http.ServeContent`.
    -   **Body:** The raw binary data of the requested file.

-   **`400 Bad Request`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Missing object ID in query parameter 'id'".
    -   *Reason:* The `id` query parameter was not provided.

-   **`401 Unauthorized`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Unauthorized or Invalid Token".
    -   *Reason:* Authentication token is missing, invalid, or expired.

-   **`403 Forbidden`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Forbidden".
    -   *Reason:* The authenticated user (derived from the token) does not own this object or is not permitted to access it.

-   **`404 Not Found`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Object metadata not found" or "Object data not found".
    -   *Reason:* The object corresponding to the provided `id` (for the authenticated user) does not exist on the server.

-   **`405 Method Not Allowed`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Method not allowed".
    -   *Reason:* An HTTP method other than `GET` was used.

-   **`500 Internal Server Error`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** Error message (e.g., "Failed to read object metadata", "Failed to parse object metadata", "Failed to read object").
    -   *Reason:* Server-side error during file retrieval or metadata processing.

#### Example `curl`

```bash
# Replace <YOUR_AUTH_TOKEN> and <OBJECT_ID_FROM_UPLOAD> (which is the objectFileUUID) with actual values
curl -X GET \
  -H "Authorization: Bearer <YOUR_AUTH_TOKEN>" \
  "http://<your-server-address>/objects/download?id=<OBJECT_ID_FROM_UPLOAD>" \
  -o downloaded_file_name.ext # Saves the downloaded file with this name
```

---

### 3. Get Object Hash

Retrieves the SHA256 hash of a stored object.

-   **Method:** `GET`
-   **Path:** `/objects/hash`
-   **Description:** Fetches the pre-calculated SHA256 hash for a given `objectID` (the file's specific UUID).
-   **Authentication:** Required.

#### Query Parameters

-   **`id`** (string, required): The `objectID` (file's specific UUID) of the object whose hash is to be retrieved.

#### Request

-   **Headers:**
    -   `Authorization: Bearer <YOUR_AUTH_TOKEN>` (or other configured auth mechanism)

#### Responses

-   **`200 OK`**
    -   **Content-Type:** `application/json`
    -   **Body:**
        ```json
        {
          "objectID": "<OBJECT_FILE_UUID>",
          "sha256": "<SHA256_HASH_STRING>"
        }
        ```
        -   `objectID`: The identifier of the object (this is the file's specific UUID).
        -   `sha256`: The SHA256 hash of the object's data file.

-   **`400 Bad Request`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Missing object ID in query parameter 'id'".
    -   *Reason:* The `id` query parameter was not provided or was empty.

-   **`401 Unauthorized`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Unauthorized or Invalid Token".
    -   *Reason:* Authentication token is missing, invalid, or expired.

-   **`403 Forbidden`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Forbidden".
    -   *Reason:* The authenticated user (derived from the token) does not own this object or is not permitted to access its hash.

-   **`404 Not Found`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Object metadata not found" or "Object hash not found".
    -   *Reason:* The object or its hash corresponding to the provided `id` (for the authenticated user) does not exist.

-   **`405 Method Not Allowed`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Method not allowed".
    -   *Reason:* An HTTP method other than `GET` was used.

-   **`500 Internal Server Error`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Failed to read object metadata" or "Failed to read object hash".
    -   *Reason:* Server-side error during metadata or hash file retrieval.

#### Example `curl`

```bash
# Replace <YOUR_AUTH_TOKEN> and <OBJECT_ID> (which is the objectFileUUID) with actual values
curl -X GET \
  -H "Authorization: Bearer <YOUR_AUTH_TOKEN>" \
  "http://<your-server-address>/objects/hash?id=<OBJECT_ID>"
```

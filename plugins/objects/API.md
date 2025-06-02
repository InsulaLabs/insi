# Objects Plugin API Documentation

## Overview

The Objects Plugin provides a simple binary object storage solution. It allows authenticated users to upload files, which are then stored on the server with associated metadata. These files can later be downloaded using a unique object ID.

The plugin prefixes its routes with `/objects`. So, an endpoint like `/upload` will be accessible at `/objects/upload`.

### Object ID

When a file is uploaded, it is assigned an `objectID`. This ID is a string composed of the `userUUID` (from the authentication token) and a newly generated `objectUUID` for the file, formatted as: `userUUID:objectUUID`. This `objectID` is required to download the file.

## Authentication

All endpoints require a valid authentication token. The method of providing this token (e.g., `Authorization: Bearer <token>` header, cookie) depends on the main application's authentication mechanism, which is handled by the `RT_ValidateAuthToken` interface function. Ensure the client sends the token as expected by the application.

## Endpoints

### 1. Upload File

Uploads a binary file to the object storage.

-   **Method:** `POST`
-   **Path:** `/objects/upload`
-   **Description:** Accepts a multipart/form-data request containing the file to be uploaded. The file is stored, and metadata (including original filename, size, upload date, content type, user UUID, and object UUID) is saved.
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
          "objectID": "userUUID:objectUUID"
        }
        ```
        -   `objectID`: The unique identifier for the uploaded object. Use this ID to download the file.

-   **`400 Bad Request`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** Error message (e.g., "Error parsing form data: ...", "Error retrieving file: ...").
    -   *Reason:* Malformed request, missing file in form-data, or upload size exceeds the server limit (currently 32MB).

-   **`401 Unauthorized`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Unauthorized" or "Unauthorized or Invalid Token".
    -   *Reason:* Authentication token is missing, invalid, or expired.

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
-   **Description:** Retrieves a file using its `objectID`.
-   **Authentication:** Required.

#### Request

-   **Headers:**
    -   `Authorization: Bearer <YOUR_AUTH_TOKEN>` (or other configured auth mechanism)
-   **Query Parameters:**
    -   **`id`** (string, required): The `objectID` of the file to download (e.g., `userUUID:objectUUID`).

#### Responses

-   **`200 OK`**
    -   **Headers:**
        -   `Content-Type`: The original content type of the uploaded file (e.g., `image/jpeg`, `application/pdf`). Defaults to `application/octet-stream` if the original type isn't available in metadata.
        -   `Content-Disposition: attachment; filename="<ORIGINAL_FILENAME>"`: Suggests the browser to download the file with its original name.
        -   Other headers like `Content-Length`, `ETag`, `Last-Modified` may be set by `http.ServeContent`.
    -   **Body:** The raw binary data of the requested file.

-   **`400 Bad Request`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Missing object ID".
    -   *Reason:* The `id` query parameter was not provided.

-   **`401 Unauthorized`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Unauthorized or Invalid Token".
    -   *Reason:* Authentication token is missing, invalid, or expired.

-   **`404 Not Found`**
    -   **Content-Type:** `text/plain; charset=utf-8`
    -   **Body:** "Object metadata not found" or "Object data not found".
    -   *Reason:* The object corresponding to the provided `id` does not exist on the server (either its metadata or the actual file data is missing).

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
# Replace <YOUR_AUTH_TOKEN> and <OBJECT_ID_FROM_UPLOAD> with actual values
curl -X GET \
  -H "Authorization: Bearer <YOUR_AUTH_TOKEN>" \
  "http://<your-server-address>/objects/download?id=<OBJECT_ID_FROM_UPLOAD>" \
  -o downloaded_file_name.ext # Saves the downloaded file with this name
```

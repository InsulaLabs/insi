### **Checklist for API Key & Alias System Testing**

#### I. Primary API Key Lifecycle
-   [x] ✅ **Create:** A root user can create a new API key.
-   [x] ✅ **Verify (Success):** `api verify` succeeds for a newly created, valid key.
-   [x] ✅ **Delete:** A root user can delete an existing API key.
-   [x] ✅ **Verify (After Deletion):** `api verify` fails for a key that has been deleted (after the tombstone runner has executed).
-   [x] ✅ **Verify (Invalid):** `api verify` fails for a random, non-existent key.
-   [x] ✅ **Delete (Non-Existent):** `api delete` should fail gracefully when trying to delete a non-existent key.
-   [x] ✅ **Create/Delete (Non-Root):** `api add` and `api delete` commands should fail if attempted without the `--root` flag.

#### II. Resource Limits
-   [x] ✅ **Get Own Limits:** A key can retrieve its own default limits.
-   [x] ✅ **Set Limits:** A root user can set custom resource limits for a specific key.
-   [x] ✅ **Verify Set Limits:** A key can retrieve its new, updated limits after they've been set.
-   [x] ✅ **Get Others' Limits (As Root):** A root user can retrieve the limits for any key.
-   [x] ✅ **Get Others' Limits (As Non-Root):** A non-root user is prevented from getting the limits of another key.

#### III. Alias Lifecycle & Functionality
-   [x] ✅ **Create Alias:** A primary key can create a new alias for itself.
-   [x] ✅ **List Aliases:** A primary key can list all its active aliases, and the list content is accurate.
-   [x] ✅ **Alias of an Alias:** The system correctly prevents an alias from creating another alias.
-   [x] ✅ **Alias Limit:** The system should prevent a key from creating more than `MaxAliasesPerKey` (16) aliases, failing on the 17th attempt.
-   [x] ✅ **Root Key Alias Attempt:** The system should prevent the system-level root key from creating an alias for itself.

#### IV. Alias Deletion & Security
-   [x] ✅ **Delete by Parent:** A primary key can successfully delete one of its aliases.
-   [x] ✅ **Verify List After Deletion:** A deleted alias is correctly removed from the `alias list` output.
-   [x] ✅ **Verify Key Invalid After Deletion:** A deleted alias key is no longer valid for authentication.
-   [x] ✅ **Verify Sibling Alias Unaffected:** Deleting one alias does not affect other aliases of the same primary key.
-   [x] ✅ **Delete by Alias Itself:** An alias should be prevented from deleting itself. The `alias delete` command should fail if authenticated with the alias key being deleted.
-   [x] ✅ **Delete by Other Key:** An unrelated API key should be prevented from deleting an alias that doesn't belong to it.
-   [x] ✅ **Delete Non-Existent Alias:** The `alias delete` command should fail gracefully when trying to delete a non-existent alias.

#### V. Data Scoping & Resource Inheritance
-   [x] ✅ **Shared Data Scope (Read/Write):** An alias can read data created by its parent and write data readable by its parent and sibling aliases.
-   [x] ✅ **Cross-Alias Data Access:** Multiple aliases of the same parent can read and write to each other's data within the shared scope. (e.g., alias1 sets a value, alias2 reads it, alias3 updates it, and the parent can see the final result).
-   [x] ✅ **Shared Limits:** An alias should operate under the resource limits of its parent. (e.g., set a low disk limit on the parent, then have the alias attempt to upload a large blob and expect it to fail).

#### VI. Cleanup on Primary Key Deletion (Tombstone)
-   [x] ✅ **Parent Key Deletion:** A primary key can be deleted, which triggers the tombstone process.
-   [x] ✅ **Verify Alias Invalidation:** After a primary key is deleted and the cleanup process runs, all of its former aliases are invalidated and can no longer be used.
-   [x] ✅ **Verify Data Deletion:** This is difficult to verify with an external script, but the core goal of the tombstone is that all data associated with the key's `DataScopeUUID` is deleted from the store. We do this by generating blobs (a record that must be manually cleaned up by the server) and then deleting the key. After a period, we check to ensure the blobs are gone.

### How LDAP Group Mapping Controls Login Access

The comment you quoted is correct — here's exactly how it works:

#### When `LDAP_GROUP_MAPPING` is set (non-empty JSON):
**Only users who belong to at least one explicitly listed group can log in.** If a user authenticates successfully against LDAP but none of their `memberOf` groups match any key in the mapping, `ldap_login()` returns `None` and login is denied.

The relevant code path (in both `ldap_auth.py` line 139–143 and the Dockerfile inline equivalent):

```python
if not matched_role:
    logging.info(f"LDAP user {email} authenticated but not in any mapped group.")
    if group_mapping:       # <-- THIS is the gate
         return None        # DENIED — valid LDAP creds but no matching group
    matched_role = "normal" # only reached if group_mapping is empty/unset
```

So **your desired behavior (only explicitly listed groups allowed) is already the default** whenever you set `LDAP_GROUP_MAPPING` to a non-empty JSON object.

#### When `LDAP_GROUP_MAPPING` is empty/unset:
All LDAP-authenticated users are allowed in as `"normal"` — no group filtering at all.

---

### How RAGFlow Roles Map to LDAP Config Values

RAGFlow has **two separate dimensions** of privilege:

#### Dimension 1: System-wide flag (`is_superuser`)
| `is_superuser` | Meaning |
|---|---|
| `True` | Full system admin — can manage all tenants, all users, everything |
| `False` | Regular user — can only see/manage their own tenant(s) |

#### Dimension 2: Per-tenant role (`UserTenantRole`)
From `api/db/__init__.py`:
```python
class UserTenantRole(StrEnum):
    OWNER = 'owner'    # Created the tenant — full control of that tenant
    ADMIN = 'admin'    # Can manage knowledgebases, chats, etc. in that tenant
    NORMAL = 'normal'  # Regular member — limited access within the tenant
    INVITE = 'invite'  # Pending invitation
```

#### How LDAP mapping values control both dimensions:

| LDAP mapping value | `is_superuser` | Tenant role | What it means |
|---|---|---|---|
| `"superuser"` | `True` | `ADMIN` in group tenants | System-wide admin + admin in any group-synced tenants |
| `"admin"` | `False` | `ADMIN` in group tenants | Regular user but with admin privileges in group-synced tenants |
| `"normal"` | `False` | `NORMAL` in group tenants | Regular member of group-synced tenants |

**Note about OWNER:** The `OWNER` role is **never** assigned via LDAP mapping. OWNER is only set when a user *creates* a tenant (via `user_register`). Every LDAP user automatically becomes OWNER of their own personal tenant (created on first login), but they'll be ADMIN or NORMAL in *group* tenants.

---

### Practical Example

If you want **only** members of three specific AD groups to log in:

```
LDAP_GROUP_MAPPING={"CN=RagFlow-Superadmins,OU=Groups,DC=corp,DC=com": "superuser", "CN=RagFlow-Admins,OU=Groups,DC=corp,DC=com": "admin", "CN=RagFlow-Users,OU=Groups,DC=corp,DC=com": "normal"}
```

Result:
- `RagFlow-Superadmins` members → `is_superuser=True`, system-wide admin
- `RagFlow-Admins` members → regular user, but `ADMIN` role in group tenants
- `RagFlow-Users` members → regular user, `NORMAL` role in group tenants
- **Anyone else** → login denied, even if their LDAP credentials are valid
- A user in **multiple** groups gets the highest role (superuser > admin > normal)

### Multi-Group & Config-Change Behavior

#### User in multiple groups with different roles
When a user belongs to several mapped groups (e.g., one mapped to `"admin"` and another to `"normal"`), the **highest** role wins. Priority: `superuser` > `admin` > `normal`. The `is_superuser` flag is set if *any* matched group maps to `"superuser"`.

#### Case-insensitive matching
All group CN comparisons — mapping lookups, prefix matching, and orphan detection — are **case-insensitive**. For example, `LDAP_GROUP_MAPPING={"Domain Admins": "superuser"}` will match an LDAP group returned as `CN=domain admins,OU=Groups,DC=corp,DC=com`. The same applies to `LDAP_TENANT_GROUP_PREFIX`.

#### Stale membership cleanup on login
On every login, the sync function compares the user's **current** LDAP groups against their DB tenant memberships. If the user was previously added to an LDAP group tenant but is no longer in that group (or the group was removed from `LDAP_GROUP_MAPPING`), the stale membership is **automatically removed** from the database. LDAP group tenants are identified by their owner email starting with `ldap_group_`. Personal tenant ownership (OWNER role) is never touched.

#### API token revocation on LDAP access loss
When an LDAP-managed user (`login_channel='ldap'`) attempts to log in and is **definitively denied access** — either removed from the LDAP directory (`user_not_found`) or no longer in any mapped group (`not_in_group`) — **all API tokens for that user's personal tenant are automatically deleted**. This prevents stale API keys from continuing to grant access after the user has been revoked in LDAP.

**Wrong password attempts do NOT trigger token revocation.** If a user simply mistypes their password (`bad_password`), their API tokens are preserved. This avoids punishing typos by destroying valid API integrations. Orphaned group tenant cleanup (via `delete_user_data()`) also removes API tokens belonging to deleted group tenants.

#### Orphaned group tenant cleanup
`LDAP_GROUP_MAPPING` is treated as the **single source of truth** for which group tenants should exist. On every successful LDAP login, the system checks all existing `ldap_group_*` dummy-owner users against the current `LDAP_GROUP_MAPPING` keys (and `LDAP_TENANT_GROUP_PREFIX`). Any group tenant whose CN no longer matches a mapping key or the prefix is **completely removed** using upstream's `delete_user_data()` function from `api/db/joint_services/user_account_service.py`.

#### Upstream-resilient cleanup
Instead of manually deleting specific tables (which would break when RAGFlow adds new features like agents, MCP servers, langfuse, memory, etc.), the cleanup delegates to upstream's `delete_user_data()`. This function handles **all** tenant-related data — knowledgebases, documents, chunks, files, agents, dialogs, conversations, API tokens, MCP servers, search records, tenant LLM configs, langfuse configs, doc metadata, memory, storage buckets, and the user/tenant records themselves. When upstream adds new features with tenant-scoped data, they update `delete_user_data()` and the LDAP cleanup automatically benefits.

#### Upstream-resilient tenant visibility
The `patched_get_joined_tenants_by_user_id` method dynamically reads the Tenant model's field list via peewee's `cls.model._meta.sorted_fields` instead of maintaining a hardcoded column list. This means if upstream adds new columns to the Tenant model (e.g., new LLM config fields), they are automatically included in the query results without any code changes on our side. The patch uses a single query with `role IN (NORMAL, ADMIN)` instead of the upstream's `role == NORMAL`, widening the filter in one place.

**Safety guard:** This cleanup only runs after a **successful** LDAP bind. If the LDAP server is unreachable or credentials are wrong (e.g., bad `LDAP_BIND_USER`/`LDAP_BIND_PASSWORD`, network outage), the login fails before reaching cleanup, so no tenants are accidentally deleted.

#### Superuser flag sync
The `is_superuser` flag is re-evaluated on every login based on current LDAP groups. If a user is removed from a `"superuser"` group in LDAP, they lose superuser status on next login.

#### Role updates within a tenant
If a user's role for a group changes (e.g., group mapping changed from `"admin"` to `"normal"`), the role is updated in the DB on next login — both upgrades and downgrades are applied.

#### Nickname sync
The user's display name (`nickname`) is re-read from LDAP on every login (from `displayName`, `cn`, or `sAMAccountName`). If it changed in the directory, RAGFlow updates the local record automatically.

#### LDAP injection protection
User-supplied input (the email/username typed at login) is sanitized via `ldap3.utils.conv.escape_filter_chars()` before being inserted into the LDAP search filter. This prevents LDAP injection attacks using special characters like `*`, `(`, `)`, `\`, or NUL bytes.

#### Profile edit protection
LDAP is a one-way sync — nickname and password are managed by the directory and re-synced on every login. To prevent confusion (edits that silently revert on next login), LDAP-managed fields are protected at **all** update paths:

- **`/v1/user/setting` endpoint** — `UserService.update_by_id` is patched to reject `nickname` or `password` updates for users with `login_channel='ldap'`. Other profile fields (avatar, language, color scheme, timezone) remain editable.
- **`/v1/user/forget/reset-password` endpoint** — `UserService.update_user_password` is patched to reject password resets for LDAP users. Without this, the "forgot password" flow (OTP → email → reset) would set a local password that never works (LDAP bind uses the directory password).
- **Admin CLI** (`ragflow_cli update_user_password`) — Same `update_user_password` patch blocks admin-initiated password changes for LDAP users.

LDAP sync itself uses `UserService.update_user` (a different, unpatched method), so it is unaffected by these protections.

#### One-way password sync
On every successful LDAP login, the local DB password hash is compared against the current LDAP password. If they differ (i.e., the user changed their password in the directory), the local hash is silently updated to match. This ensures the local DB always reflects the current LDAP password. The sync calls the **original** (unpatched) `update_user_password` so it is not blocked by the LDAP password-change protection patch.

#### LDAP password change handling
When an LDAP user's password changes in the directory:
- **New password** → LDAP bind succeeds → login works, local hash updated to match
- **Old password** → LDAP bind fails → local fallback is **blocked** (user has `login_channel='ldap'`) → login denied

The local fallback guard prevents stale password hashes in the DB from being used as a backdoor.

#### Connection safety
The LDAP service-account connection is wrapped in `try/finally` to guarantee `conn.unbind()` runs even if an unexpected exception occurs mid-search or mid-authentication. This prevents connection leaks.

---

### LDAP Group Email Domain

When RAGFlow creates a "dummy" owner user for an LDAP group tenant, it generates an email like `ldap_group_<sanitized_cn>@<domain>`. By default the domain is `ragflow.org`.

To customize this, set:

```
LDAP_GROUP_EMAIL_DOMAIN=yourcompany.com
```

This produces emails like `ldap_group_PRC_BMI_PB_E3_AppAdmin_CYBERDS@yourcompany.com` instead of `…@ragflow.org`.

---

### Patch Resilience Reference

The LDAP integration works by monkey-patching 5 upstream methods and calling 2 upstream functions directly. All patches are designed to be resilient to upstream RAGFlow code changes.

#### Patched Methods

| Patch | Target | Pattern | Resilience |
|---|---|---|---|
| `patched_query_user` | `UserService.query_user` | Transparent wrapper | ✅ High |
| `patched_query` | `UserService.query` | Transparent wrapper | ✅ High |
| `patched_user_update_by_id` | `UserService.update_by_id` | Pre-check guard | ✅ High |
| `patched_update_user_password` | `UserService.update_user_password` | Pre-check guard | ✅ High |
| `patched_get_joined_tenants_by_user_id` | `TenantService.get_joined_tenants_by_user_id` | Dynamic field query | ✅ High |

#### Direct Upstream Calls

| Function | Source | Resilience |
|---|---|---|
| `user_register(user_id, user_dict)` | `sys.modules['api.apps.user'].user_register` | ✅ High — extra dict keys ignored by peewee; missing required keys fail loudly |
| `delete_user_data(user_id)` | `api.db.joint_services.user_account_service` | ✅ High — upstream maintains this for all tenant-related cleanup |

#### Patch Patterns Explained

- **Transparent wrapper**: Calls the original method and only intercepts specific cases (e.g., LDAP users). All upstream logic changes are inherited automatically since the original is always called for non-LDAP paths.
- **Pre-check guard**: Performs a lightweight check (is user LDAP-managed? are blocked fields present?) before delegating 100% to the original. Never modifies arguments or return values.
- **Dynamic field query**: Reads Tenant model fields via `cls.model._meta.sorted_fields` at runtime instead of a hardcoded list. Automatically picks up columns upstream adds. Falls back to original on any exception.

#### Resilience to Specific Upstream Changes

| Upstream change | Impact |
|---|---|
| New columns added to `User` or `Tenant` model | ✅ No impact — patches use model objects or dynamic field reads |
| Internal logic changes in patched methods | ✅ No impact — originals are called for all non-LDAP paths |
| New features added (agents, MCP servers, etc.) | ✅ No impact — `delete_user_data()` handles cleanup; `_meta.sorted_fields` includes new columns |
| Signature changes to patched methods | ⚠️ Low risk — all patched signatures (`email, password`, `pid, data`, `user_id, new_password`, `user_id`) are stable core APIs used across the entire codebase |
| `login_channel` field removed from User model | ✅ Safe — `getattr(user, 'login_channel', '')` returns `''`, guards silently bypassed |
| Method renamed or removed | ✅ Safe — `original_get_joined_tenants` is checked for `None` before patching; others would cause import-time errors (easily caught) |

#### Guard Conditions

All patches are conditional — they are only applied when `LDAP_ENABLED=true`:

```python
if LDAP_ENABLED and original_query_user:
    UserService.query_user = classmethod(patched_query_user)
    # ... other patches ...
```

When `LDAP_ENABLED` is `false` (the default), none of these patches are applied and RAGFlow runs 100% vanilla.

---

### Summary

| Scenario | Login allowed? | Role assigned | API tokens |
|---|---|---|---|
| `LDAP_GROUP_MAPPING` set, user in a listed group | ✅ Yes | Per the mapping value | Kept |
| `LDAP_GROUP_MAPPING` set, user NOT in any listed group | ❌ **Denied** | — | **Revoked** |
| `LDAP_GROUP_MAPPING` empty/unset | ✅ Yes (all LDAP users) | `normal` | Kept |
| User removed from LDAP directory | ❌ **Denied** | — | **Revoked** |
| User's LDAP password changed (old password used) | ❌ **Denied** | — | Kept (wrong password, not revoked) |
| Wrong password (typo) | ❌ **Denied** | — | Kept (not revoked) |

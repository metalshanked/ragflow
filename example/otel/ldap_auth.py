import os
import json
import logging
import ssl
import base64
from api.db.services.user_service import UserService, UserTenantService, TenantService
from api.db.services.api_service import APITokenService
from api.db import UserTenantRole
from api.db.db_models import UserTenant, User
from common.constants import StatusEnum
from common.misc_utils import get_uuid

# user_register is imported lazily inside functions that need it, because
# importing api.apps.user_app at module level triggers Flask/Quart Blueprint
# decorators (@manager.route) which fail if the app hasn't been initialised yet.
# ldap_auth.py is imported very early (line 1 of ragflow_server.py).

# Try to import ldap3, if not available, log warning and skip
try:
    from ldap3 import Server, Connection, ALL, SUBTREE, Tls
    from ldap3.utils.conv import escape_filter_chars
    LDAP_AVAILABLE = True
except ImportError:
    LDAP_AVAILABLE = False
    logging.warning("ldap3 module not found. LDAP authentication will be disabled.")

# Check environment
LDAP_ENABLED = os.environ.get("LDAP_ENABLED", "False").lower() == "true"

# Save originals before patching
original_query_user = UserService.query_user
original_get_joined_tenants = getattr(TenantService, "get_joined_tenants_by_user_id", None)

def ldap_login(email, password):
    if not LDAP_ENABLED:
        return None
    
    if not LDAP_AVAILABLE:
        logging.error("LDAP_ENABLED is true but ldap3 module is not installed.")
        return None

    ldap_server_url = os.environ.get("LDAP_SERVER")
    ldap_bind_user = os.environ.get("LDAP_BIND_USER")
    ldap_bind_password = os.environ.get("LDAP_BIND_PASSWORD")
    ldap_base_dn = os.environ.get("LDAP_BASE_DN")
    ldap_user_filter = os.environ.get("LDAP_USER_FILTER", "(mail=%s)")
    ldap_group_mapping_str = os.environ.get("LDAP_GROUP_MAPPING", "{}")
    ldap_verify_certs = os.environ.get("LDAP_VERIFY_CERTS", "True").lower() == "true"

    if not ldap_server_url or not ldap_base_dn:
        logging.error("LDAP configuration incomplete (LDAP_SERVER, LDAP_BASE_DN required).")
        return None

    try:
        group_mapping = json.loads(ldap_group_mapping_str)
    except json.JSONDecodeError:
        logging.error("Invalid JSON in LDAP_GROUP_MAPPING.")
        return None

    tls_ctx = None
    if ldap_server_url.lower().startswith("ldaps://") and not ldap_verify_certs:
        try:
            tls_ctx = Tls(validate=ssl.CERT_NONE)
            logging.warning("LDAP: Certificate verification disabled (LDAPS).")
        except Exception as e:
            logging.error(f"LDAP: Failed to create TLS context: {e}")

    server = Server(ldap_server_url, get_info=ALL, tls=tls_ctx)
    
    # 1. Bind with service account (or anonymous) to search for user
    try:
        if ldap_bind_user and ldap_bind_password:
            conn = Connection(server, user=ldap_bind_user, password=ldap_bind_password, auto_bind=True)
        else:
            conn = Connection(server, auto_bind=True)
    except Exception as e:
        logging.error(f"LDAP Bind failed: {e}")
        return None

    # 2. Search for user
    # Escape user input to prevent LDAP injection (*, (, ), \, NUL, etc.)
    search_filter = ldap_user_filter % escape_filter_chars(email)
    try:
        conn.search(ldap_base_dn, search_filter, attributes=['cn', 'mail', 'memberOf', 'sAMAccountName', 'displayName'])
        
        if not conn.entries:
            logging.info(f"LDAP user not found for email: {email}")
            return "user_not_found"
        
        user_entry = conn.entries[0]
        user_dn = user_entry.entry_dn
        user_attributes = user_entry.entry_attributes_as_dict
        
        # Get nickname from cn or displayName or sAMAccountName or mail
        nickname = user_attributes.get('displayName', [None])[0] or \
                   user_attributes.get('cn', [None])[0] or \
                   user_attributes.get('sAMAccountName', [None])[0] or \
                   email.split('@')[0]

        # 3. Authenticate user (Bind with user DN and password)
        # The password arriving here has been through decrypt() in user_app.py,
        # which returns base64(original_password) — NOT the raw password.
        # We must base64-decode it to get the real password for the LDAP bind.
        try:
            actual_password = base64.b64decode(password).decode('utf-8')
        except Exception:
            actual_password = password  # fallback: use as-is if not base64
        try:
            user_conn = Connection(server, user=user_dn, password=actual_password, auto_bind=True)
            user_conn.unbind()
        except Exception as e:
            logging.warning(f"LDAP authentication failed for user {user_dn}: {e}")
            return "bad_password"

        # 4. Check groups
        user_groups = user_attributes.get('memberOf', [])
        
        # Some LDAP setups don't populate memberOf on user objects
        if not user_groups:
            conn.search(ldap_base_dn, f"(member={user_dn})", attributes=['dn'])
            user_groups = [entry.entry_dn for entry in conn.entries]
    finally:
        conn.unbind()

    # Normalize groups for mapping — pick the highest role across all groups
    ROLE_PRIORITY = {"normal": 0, "admin": 1, "superuser": 2}
    matched_role = None
    is_superuser = False
    best_priority = -1
    
    if not group_mapping:
        matched_role = "normal"
    else:
        # Build a case-insensitive lookup: lowercase key -> original value
        ci_mapping = {k.lower(): v for k, v in group_mapping.items()}
        for group in user_groups:
            role = None
            if group.lower() in ci_mapping:
                role = ci_mapping[group.lower()]
            else:
                cn = group.split(',')[0]
                if cn.upper().startswith("CN="):
                    cn = cn[3:]
                if cn.lower() in ci_mapping:
                    role = ci_mapping[cn.lower()]
            
            if role:
                priority = ROLE_PRIORITY.get(role.lower(), 0)
                if priority > best_priority:
                    best_priority = priority
                    matched_role = role
                if role.lower() == "superuser":
                    is_superuser = True
    
    if not matched_role:
        logging.info(f"LDAP user {email} authenticated but not in any mapped group.")
        if group_mapping:
             return "not_in_group"
        matched_role = "normal"

    return {
        "email": email,
        "nickname": nickname,
        "is_superuser": is_superuser,
        "groups": user_groups
    }

def get_or_create_group_tenant(group_dn):
    """Get or create a tenant for an LDAP group.
    
    Uses upstream user_register() for the actual creation to stay resilient
    against upstream changes to tenant/file/LLM initialization logic.
    """
    cn = group_dn.split(',')[0]
    if cn.upper().startswith("CN="):
        cn = cn[3:]
    
    if not cn:
        return None

    tenant_name = cn[:100]
    tenant_display_name = tenant_name + "'s Kingdom"

    # Check if tenant exists by name (user_register appends "'s Kingdom")
    try:
        tenant = TenantService.model.select().where(TenantService.model.name == tenant_display_name).first()
    except Exception as e:
        logging.error(f"Error querying tenant {tenant_name}: {e}")
        return None

    if tenant:
        return tenant
    
    # Create new tenant via upstream user_register (dummy owner approach)
    logging.info(f"Creating new tenant for LDAP group: {tenant_name}")
    
    safe_name = "".join([c if c.isalnum() else "_" for c in tenant_name])
    group_email_domain = os.environ.get("LDAP_GROUP_EMAIL_DOMAIN", "ragflow.org")
    dummy_email = f"ldap_group_{safe_name}@{group_email_domain}"
    
    # Check if dummy user already exists
    dummy_user = UserService.query_user_by_email(dummy_email)
    if dummy_user:
        dummy_user = dummy_user[0]
        tenant = TenantService.get_info_by(dummy_user.id)
        if tenant:
             t_id = tenant[0]['tenant_id']
             return TenantService.model.get_by_id(t_id)
    
    # Use upstream user_register to create user + tenant + files + LLMs
    import sys
    user_register = sys.modules['api.apps.user'].user_register
    user_id = get_uuid()
    user_dict = {
        "email": dummy_email,
        "nickname": tenant_name,
        "password": get_uuid(),
        "is_superuser": False,
    }

    try:
        result = user_register(user_id, user_dict)
        if not result:
            logging.error(f"user_register failed for group tenant {tenant_name}")
            return None
        return TenantService.model.get_by_id(user_id)
    except Exception as e:
        logging.error(f"Error creating tenant for group {tenant_name}: {e}")
        return None

def sync_user_to_group_tenants(user, groups):
    if not groups:
        return

    ldap_group_mapping_str = os.environ.get("LDAP_GROUP_MAPPING", "{}")
    try:
        group_mapping = json.loads(ldap_group_mapping_str)
    except Exception:
        group_mapping = {}

    tenant_group_prefix = os.environ.get("LDAP_TENANT_GROUP_PREFIX", "")

    tenant_targets = {}

    # Build case-insensitive lookup for group mapping
    ci_mapping = {k.lower(): v for k, v in group_mapping.items()}
    ci_prefix = tenant_group_prefix.lower()

    for group_dn in groups:
        is_match = False
        target_role = UserTenantRole.NORMAL
        
        role_str = None
        if group_dn.lower() in ci_mapping:
            role_str = ci_mapping[group_dn.lower()]
            is_match = True
        
        cn = group_dn.split(',')[0]
        if cn.upper().startswith("CN="):
            cn = cn[3:]
            
        if not role_str and cn.lower() in ci_mapping:
            role_str = ci_mapping[cn.lower()]
            is_match = True
            
        if not is_match and ci_prefix and cn.lower().startswith(ci_prefix):
            is_match = True
            
        if is_match:
            if role_str:
                if role_str.lower() in ("superuser", "admin"):
                    target_role = UserTenantRole.ADMIN
            
            if cn not in tenant_targets:
                tenant_targets[cn] = { "role": target_role, "group_dn": group_dn }
            else:
                if tenant_targets[cn]["role"] == UserTenantRole.NORMAL and target_role == UserTenantRole.ADMIN:
                    tenant_targets[cn]["role"] = UserTenantRole.ADMIN

    if not tenant_targets:
        logging.info(f"User {user.email} belongs to {len(groups)} LDAP groups, but none matched the tenant filter.")
    else:
        logging.info(f"Syncing user {user.email} to {len(tenant_targets)} matched group tenants.")

    synced_tenant_ids = set()

    for cn, info in tenant_targets.items():
        group_dn = info["group_dn"]
        target_role = info["role"]
        
        tenant = get_or_create_group_tenant(group_dn)
        if not tenant:
            continue

        synced_tenant_ids.add(tenant.id)

        try:
            ut = UserTenantService.filter_by_tenant_and_user_id(tenant.id, user.id)
            if not ut:
                logging.info(f"Adding user {user.email} to tenant {tenant.name} with role {target_role}")
                UserTenantService.save(
                    id=get_uuid(),
                    user_id=user.id,
                    tenant_id=tenant.id,
                    role=target_role,
                    invited_by=user.id,
                    status="1"
                )
            else:
                if ut.role != UserTenantRole.OWNER and ut.role != target_role:
                    logging.info(f"Updating user {user.email} role in tenant {tenant.name} to {target_role}")
                    ut.role = target_role
                    ut.save()
        except Exception as e:
            logging.error(f"Error syncing user {user.email} to tenant {tenant.name}: {e}")

    # Remove user from LDAP group tenants they no longer belong to.
    # LDAP group tenants are identified by their owner email starting with "ldap_group_".
    try:
        stale = list(
            UserTenant.select()
            .join(User, on=(User.id == UserTenant.tenant_id))
            .where(
                (UserTenant.user_id == user.id)
                & (UserTenant.role != UserTenantRole.OWNER)
                & (UserTenant.status == StatusEnum.VALID.value)
                & (User.email.startswith("ldap_group_"))
            )
        )
        for ut in stale:
            if ut.tenant_id not in synced_tenant_ids:
                logging.info(f"Removing user {user.email} from stale LDAP group tenant {ut.tenant_id}")
                ut.delete_instance()
    except Exception as e:
        logging.error(f"Error cleaning stale LDAP group memberships for {user.email}: {e}")

def cleanup_orphaned_group_tenants():
    """Remove LDAP group tenants whose groups no longer appear in config.

    Uses LDAP_GROUP_MAPPING and LDAP_TENANT_GROUP_PREFIX as the source of truth.
    Must only be called after a successful LDAP bind so we know the directory is
    reachable — avoids accidental cleanup caused by network/credential failures.
    """
    ldap_group_mapping_str = os.environ.get("LDAP_GROUP_MAPPING", "{}")
    try:
        group_mapping = json.loads(ldap_group_mapping_str)
    except Exception:
        return  # Can't parse mapping — skip cleanup to be safe

    tenant_group_prefix = os.environ.get("LDAP_TENANT_GROUP_PREFIX", "")

    # Build set of valid group CNs from mapping keys (lowercased for case-insensitive match)
    valid_cns = set()
    for key in group_mapping:
        cn = key.split(',')[0]
        if cn.upper().startswith("CN="):
            cn = cn[3:]
        valid_cns.add(cn.lower())

    ci_prefix = tenant_group_prefix.lower()

    # Find all ldap_group_ dummy owner users
    try:
        dummy_users = list(
            User.select().where(User.email.startswith("ldap_group_"))
        )
    except Exception as e:
        logging.error(f"Error querying ldap_group_ users for orphan cleanup: {e}")
        return

    for dummy_user in dummy_users:
        original_cn = dummy_user.nickname  # nickname = original group CN

        # Check if still valid in current config (case-insensitive)
        is_valid = original_cn.lower() in valid_cns
        if not is_valid and ci_prefix and original_cn.lower().startswith(ci_prefix):
            is_valid = True

        if is_valid:
            continue

        # Orphaned group tenant — remove it completely using upstream's
        # delete_user_data() so new features (agents, MCP servers, etc.)
        # are automatically cleaned up without maintaining a manual list.
        logging.info(f"Removing orphaned LDAP group tenant: {dummy_user.email} (group={original_cn})")
        try:
            # delete_user_data requires: is_active=INACTIVE, is_superuser=False
            from common.constants import ActiveEnum
            if dummy_user.is_active == ActiveEnum.ACTIVE.value:
                UserService.update_by_id(dummy_user.id, {"is_active": ActiveEnum.INACTIVE.value})
            if dummy_user.is_superuser:
                UserService.update_by_id(dummy_user.id, {"is_superuser": False})

            from api.db.joint_services.user_account_service import delete_user_data
            result = delete_user_data(dummy_user.id)
            if result.get("success"):
                logging.info(f"Successfully removed orphaned group tenant {original_cn} ({dummy_user.email})")
            else:
                logging.error(f"delete_user_data failed for {dummy_user.email}: {result.get('message')}")
        except Exception as e:
            logging.error(f"Error removing orphaned group tenant {dummy_user.email}: {e}")

def patched_query_user(cls, email, password):
    # Try LDAP first
    ldap_info = ldap_login(email, password)
    
    if isinstance(ldap_info, dict):
        logging.info(f"LDAP Login success for {email}")
        users = cls.query_user_by_email(email)
        user = None
        if not users:
            # Register via upstream user_register — resilient to signature changes
            logging.info(f"Creating new user from LDAP: {email}")
            import sys
            user_register = sys.modules['api.apps.user'].user_register
            user_id = get_uuid()
            user_dict = {
                "email": email,
                "nickname": ldap_info["nickname"],
                "password": password,
                "is_superuser": ldap_info["is_superuser"],
                "login_channel": "ldap",
            }

            try:
                result = user_register(user_id, user_dict)
                if not result:
                    logging.error(f"user_register failed for LDAP user {email}")
                    return None
                user = cls.query_user_by_email(email)[0]
            except Exception as e:
                logging.error(f"Error registering LDAP user {email}: {e}")
                return None
            
        else:
            user = users[0]
            updates = {}
            if user.is_superuser != ldap_info["is_superuser"]:
                updates["is_superuser"] = ldap_info["is_superuser"]
                logging.info(f"Updating superuser status for {email} to {ldap_info['is_superuser']}")
            if user.nickname != ldap_info["nickname"]:
                updates["nickname"] = ldap_info["nickname"]
                logging.info(f"Updating nickname for {email} to {ldap_info['nickname']}")
            if getattr(user, 'login_channel', '') != 'ldap':
                updates["login_channel"] = "ldap"
            if updates:
                UserService.update_user(user.id, updates)
                for k, v in updates.items():
                    setattr(user, k, v)

            # One-way password sync: keep local DB hash in sync with LDAP.
            # After a successful LDAP bind the `password` arg holds the current
            # LDAP password (base64-encoded by decrypt()).  We call the *original*
            # update_user_password (not the patched one, which blocks LDAP users)
            # so the local hash always matches the directory.
            try:
                from werkzeug.security import check_password_hash as _chk
                if not _chk(user.password, str(password)):
                    logging.info(f"Syncing LDAP password to local DB for {email}")
                    original_update_user_password(user.id, password)
            except Exception as e:
                logging.warning(f"Failed to sync LDAP password for {email}: {e}")

        # Sync groups to tenants
        if "groups" in ldap_info:
            sync_user_to_group_tenants(user, ldap_info["groups"])

        # Clean up group tenants for groups removed from LDAP_GROUP_MAPPING.
        # Safe to call here — we just completed a successful LDAP bind, so
        # config/network issues are not the cause of any missing groups.
        try:
            cleanup_orphaned_group_tenants()
        except Exception as e:
            logging.error(f"Error during orphaned group tenant cleanup: {e}")

        return user

    # LDAP login returned a failure reason.
    # Only revoke API tokens when the user is definitively denied access
    # (removed from directory or not in any mapped group).  Do NOT revoke
    # on a simple wrong-password attempt — that would punish typos.
    if ldap_info in ("user_not_found", "not_in_group"):
        try:
            existing = cls.query_user_by_email(email)
            if existing and getattr(existing[0], 'login_channel', '') == 'ldap':
                user_id = existing[0].id
                deleted = APITokenService.delete_by_tenant_id(user_id)
                if deleted:
                    logging.info(
                        f"Revoked {deleted} API token(s) for LDAP user {email} "
                        f"(reason: {ldap_info})."
                    )
        except Exception as e:
            logging.error(f"Error revoking API tokens for {email}: {e}")

    # Fallback to local — but NOT for LDAP-managed users.
    # If an LDAP user's password changed in the directory, ldap_login() correctly
    # rejects the old password.  Without this guard the old password (still hashed
    # in the local DB) would pass original_query_user, creating a stale-password
    # backdoor.
    try:
        existing = cls.query_user_by_email(email)
        if existing and getattr(existing[0], 'login_channel', '') == 'ldap':
            logging.warning(f"LDAP authentication failed for {email}; local fallback blocked for LDAP-managed user.")
            return None
    except Exception:
        pass
    return original_query_user(email, password)

def patched_get_joined_tenants_by_user_id(cls, user_id):
    """Widen upstream's NORMAL-only query to include ADMIN memberships.

    Dynamically reads all Tenant model fields via peewee's model metadata
    so upstream column additions are automatically picked up — no hardcoded
    field list to maintain.  Uses a single query with role IN (NORMAL, ADMIN)
    instead of two separate queries merged together.
    """
    from api.db.db_models import DB
    try:
        with DB.connection_context():
            # Build field list dynamically from the Tenant model metadata.
            # This automatically picks up any columns upstream adds later.
            fields = []
            for f in cls.model._meta.sorted_fields:
                if f.name == "id":
                    fields.append(f.alias("tenant_id"))
                else:
                    fields.append(f)
            fields.append(UserTenant.role)

            return list(
                cls.model.select(*fields)
                .join(
                    UserTenant,
                    on=(
                        (cls.model.id == UserTenant.tenant_id)
                        & (UserTenant.user_id == user_id)
                        & (UserTenant.status == StatusEnum.VALID.value)
                        & (UserTenant.role << [UserTenantRole.NORMAL, UserTenantRole.ADMIN])
                    ),
                )
                .where(cls.model.status == StatusEnum.VALID.value)
                .dicts()
            )
    except Exception:
        # Fallback to original if anything goes wrong
        return original_get_joined_tenants(user_id) if original_get_joined_tenants else []

# --- Patch UserService.query so first-time LDAP users pass the
#     "is email registered?" gate in the login endpoint (user_app.py
#     line ~102).  Without this, the endpoint returns "Email … is not
#     registered!" before query_user (where LDAP auto-provisioning
#     lives) is ever called.
original_query = UserService.query

def patched_query(cls, *args, **kwargs):
    """Wrapper around UserService.query.

    When LDAP is enabled and an email-only query returns no rows,
    return a lightweight placeholder so the login flow proceeds to
    query_user() where the real LDAP bind + auto-registration happens.
    """
    result = original_query(*args, **kwargs)
    if result:
        return result
    # Only intercept email-only lookups (the login-gate pattern)
    if LDAP_ENABLED and kwargs.get("email") and len(kwargs) == 1 and not args:
        from api.db.db_models import User
        placeholder = User()
        placeholder.email = kwargs["email"]
        placeholder.id = "ldap_placeholder"
        return [placeholder]
    return result

# --- Patch UserService.update_user_password to block password resets for LDAP users.
#     The /forget/reset-password endpoint and admin CLI both call
#     update_user_password directly (bypassing update_by_id).
original_update_user_password = UserService.update_user_password

def patched_update_user_password(cls, user_id, new_password):
    """Block password changes for LDAP users.

    LDAP passwords are managed by the directory — local changes would
    be meaningless (LDAP bind uses the directory password, not the DB one)
    and confusing (the user thinks they changed it but login still uses
    the old LDAP password).
    """
    try:
        user = User.get_or_none(User.id == user_id)
        if user and getattr(user, 'login_channel', '') == 'ldap':
            logging.info(
                f"Blocked password change for LDAP user {user_id}. "
                f"Passwords are managed by the LDAP directory."
            )
            raise ValueError(
                "Password changes are disabled for LDAP-managed users. "
                "Your password is controlled by your LDAP directory."
            )
    except ValueError:
        raise
    except Exception:
        pass  # DB lookup failed — let the original handle it
    return original_update_user_password(user_id, new_password)

# --- Patch UserService.update_by_id to block profile edits for LDAP users.
#     The /setting endpoint calls update_by_id; LDAP sync uses update_user
#     (a different method), so this patch only affects UI-initiated edits.
original_update_by_id = UserService.update_by_id

def patched_user_update_by_id(cls, pid, data):
    """Block profile edits for LDAP users.

    LDAP is a one-way sync — nickname and password are managed by the
    directory and re-synced on every login.  Rather than letting edits
    succeed and then silently reverting them on the next login (which is
    confusing), we reject the update outright so the UI shows an error.
    """
    try:
        user = User.get_or_none(User.id == pid)
        if user and getattr(user, 'login_channel', '') == 'ldap':
            # Check if the update contains user-facing profile fields
            ldap_managed = {'nickname', 'password'}
            blocked = ldap_managed & set(data.keys())
            if blocked:
                logging.info(
                    f"Blocked UI update of LDAP-managed fields {blocked} for user {pid}. "
                    f"Profile data is managed by the LDAP directory."
                )
                raise ValueError(
                    "Profile editing is disabled for LDAP-managed users. "
                    "Nickname and password are controlled by your LDAP directory."
                )
    except ValueError:
        raise  # re-raise our own error
    except Exception:
        pass  # DB lookup failed — let the original handle it
    return original_update_by_id(pid, data)

# Apply Patches
if LDAP_ENABLED and original_query_user:
    UserService.query_user = classmethod(patched_query_user)
    logging.info("LDAP Auth Patch Applied to UserService.query_user")

    UserService.query = classmethod(patched_query)
    logging.info("LDAP Auth Patch Applied to UserService.query (login gate bypass)")

    UserService.update_by_id = classmethod(patched_user_update_by_id)
    logging.info("LDAP Auth Patch Applied to UserService.update_by_id (profile edit protection)")

    UserService.update_user_password = classmethod(patched_update_user_password)
    logging.info("LDAP Auth Patch Applied to UserService.update_user_password (password reset protection)")

    if original_get_joined_tenants is not None:
        TenantService.get_joined_tenants_by_user_id = classmethod(patched_get_joined_tenants_by_user_id)
        logging.info("TenantService.get_joined_tenants_by_user_id Patched for LDAP Admin visibility")

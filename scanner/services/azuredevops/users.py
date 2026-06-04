#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import logging

logger = logging.getLogger(__name__)


class UsersService:
    """Discovers ADO organisation users, group memberships, and PAT tokens."""

    def __init__(self, manager, http_ops):
        self.manager = manager
        self.http_ops = http_ops

    # ------------------------------------------------------------------ #
    #  Users
    # ------------------------------------------------------------------ #
    def get_all_users(self):
        """
        Fetch all users (human + service) from the org via the Graph API.

        Returns a dict keyed by the user's origin id (the GUID that appears
        in authoredBy / requestedBy blocks throughout ADO), with each value
        containing the full user record plus a ``k_id`` field for easy
        cross-referencing.

        Pagination is handled via the ``continuationToken`` header.
        """
        base_url = (
            f"https://vssps.dev.azure.com/{self.manager.organization}"
            f"/_apis/graph/users?api-version=7.1-preview.1"
        )
        all_users = []
        url = base_url

        while url:
            data, headers = self.http_ops.fetch_data_with_headers(url)
            if isinstance(data, dict) and "value" in data:
                all_users.extend(data["value"])
            elif isinstance(data, list):
                all_users.extend(data)

            # ADO returns a continuation token header for pagination
            continuation = (headers or {}).get("X-MS-ContinuationToken") or (headers or {}).get("x-ms-continuationtoken")
            if continuation:
                separator = "&" if "?" in base_url else "?"
                url = f"{base_url}{separator}continuationToken={continuation}"
            else:
                url = None

        logger.info(f"Fetched {len(all_users)} users from organisation")
        return self._build_users_lookup(all_users)

    def _build_users_lookup(self, raw_users):
        """
        Build a lookup dict keyed by the user's **origin id** (the GUID
        that appears as ``authoredBy.id``, ``requestedBy.id``, etc. across
        the scan data).

        Each entry carries a ``k_id`` alias so downstream consumers can
        join on it.
        """
        lookup = {}
        for user in raw_users:
            origin_id = user.get("originId")
            descriptor = user.get("descriptor", "")
            display_name = user.get("displayName", "")
            principal_name = user.get("principalName", "")
            domain = user.get("domain", "")
            mail_address = user.get("mailAddress", "")
            subject_kind = user.get("subjectKind", "")  # "user", "svc", etc.
            origin = user.get("origin", "")              # "aad", "vsts", etc.

            entry = {
                "k_id": origin_id,
                "descriptor": descriptor,
                "displayName": display_name,
                "principalName": principal_name,
                "mailAddress": mail_address,
                "domain": domain,
                "subjectKind": subject_kind,
                "origin": origin,
                "memberships": [],   # populated later
                "patTokens": [],     # populated later
            }

            if origin_id:
                lookup[origin_id] = entry
            else:
                # Fall back to descriptor as key
                lookup[descriptor] = entry

        return lookup

    # ------------------------------------------------------------------ #
    #  Groups & RBAC
    # ------------------------------------------------------------------ #
    def get_all_groups(self):
        """
        Fetch all groups in the organisation.
        Returns a dict keyed by group descriptor.
        """
        base_url = (
            f"https://vssps.dev.azure.com/{self.manager.organization}"
            f"/_apis/graph/groups?api-version=7.1-preview.1"
        )
        all_groups = []
        url = base_url

        while url:
            data, headers = self.http_ops.fetch_data_with_headers(url)
            if isinstance(data, dict) and "value" in data:
                all_groups.extend(data["value"])
            elif isinstance(data, list):
                all_groups.extend(data)

            continuation = (headers or {}).get("X-MS-ContinuationToken") or (headers or {}).get("x-ms-continuationtoken")
            if continuation:
                separator = "&" if "?" in base_url else "?"
                url = f"{base_url}{separator}continuationToken={continuation}"
            else:
                url = None

        logger.info(f"Fetched {len(all_groups)} groups from organisation")
        return {
            g.get("descriptor", ""): {
                "descriptor": g.get("descriptor", ""),
                "displayName": g.get("displayName", ""),
                "principalName": g.get("principalName", ""),
                "domain": g.get("domain", ""),
                "origin": g.get("origin", ""),
                "originId": g.get("originId", ""),
                "description": g.get("description", ""),
            }
            for g in all_groups
        }

    def get_user_memberships(self, user_descriptor):
        """
        Fetch the group memberships for a single user descriptor.
        Returns a list of group descriptors the user belongs to.
        """
        url = (
            f"https://vssps.dev.azure.com/{self.manager.organization}"
            f"/_apis/graph/memberships/{user_descriptor}"
            f"?api-version=7.1-preview.1&direction=up"
        )
        try:
            data = self.http_ops.fetch_data(url)
            if isinstance(data, dict) and "value" in data:
                return [m.get("containerDescriptor", "") for m in data["value"]]
            elif isinstance(data, list):
                return [m.get("containerDescriptor", "") for m in data]
            return []
        except Exception as err:
            logger.warning(f"Failed to fetch memberships for {user_descriptor}: {err}")
            return []

    def enrich_users_with_memberships(self, users_lookup, groups_lookup):
        """
        For each user, resolve their group memberships into readable names.
        Mutates users_lookup in place.
        """
        for _uid, user in users_lookup.items():
            descriptor = user.get("descriptor", "")
            if not descriptor:
                continue
            membership_descriptors = self.get_user_memberships(descriptor)
            user["memberships"] = [
                {
                    "descriptor": md,
                    "displayName": groups_lookup.get(md, {}).get("displayName", md),
                }
                for md in membership_descriptors
            ]
        logger.info("Enriched users with group memberships")
        return users_lookup

    # ------------------------------------------------------------------ #
    #  PAT Tokens (requires Token Administration scope)
    # ------------------------------------------------------------------ #
    def get_pat_tokens_for_user(self, user_descriptor):
        """
        Fetch personal access tokens for a user via the Token Admin API.

        Requires the PAT used for scanning to have the
        ``Token Administration (Read)`` scope.  If the caller lacks
        permissions the endpoint returns 401/403 and we gracefully
        return an empty list.
        """
        url = (
            f"https://vssps.dev.azure.com/{self.manager.organization}"
            f"/_apis/tokenadmin/personalaccesstokens/{user_descriptor}"
            f"?api-version=7.1-preview.1"
        )
        try:
            data = self.http_ops.fetch_data(url)
            if isinstance(data, dict) and "value" in data:
                return self._normalise_pat_list(data["value"])
            elif isinstance(data, list):
                return self._normalise_pat_list(data)
            return []
        except Exception as err:
            logger.debug(f"PAT fetch for {user_descriptor}: {err}")
            return []

    def _normalise_pat_list(self, raw_pats):
        """Return a safe subset of PAT metadata (never the token value)."""
        results = []
        for pat in raw_pats:
            results.append({
                "displayName": pat.get("displayName", ""),
                "validFrom": pat.get("validFrom", ""),
                "validTo": pat.get("validTo", ""),
                "scope": pat.get("scope", ""),
                "isValid": pat.get("isValid"),
                "tokenId": pat.get("authorizationId", ""),
            })
        return results

    def enrich_users_with_pat_tokens(self, users_lookup):
        """
        For each human user, query the Token Admin API for their PATs.
        Mutates users_lookup in place.
        """
        pat_capable_count = 0
        pat_found_count = 0
        first_failure_logged = False

        for _uid, user in users_lookup.items():
            # Only query PATs for human users
            if user.get("subjectKind") != "user":
                continue
            descriptor = user.get("descriptor", "")
            if not descriptor:
                continue

            pat_capable_count += 1
            pats = self.get_pat_tokens_for_user(descriptor)
            if pats:
                user["patTokens"] = pats
                pat_found_count += len(pats)
            elif not first_failure_logged and pat_capable_count == 1:
                # If the very first user returns empty, the PAT likely
                # lacks Token Administration scope - log once.
                logger.warning(
                    "Ensure the scanning PAT has 'Token Administration (Read)' scope "
                    "to discover user PATs."
                )
                first_failure_logged = True

        logger.info(
            f"PAT token discovery: checked {pat_capable_count} users, "
            f"found {pat_found_count} tokens total"
        )
        return users_lookup

    # ------------------------------------------------------------------ #
    #  Orchestration helper
    # ------------------------------------------------------------------ #
    def discover_users_and_access(self):
        """
        Full discovery: users → groups → memberships → PAT tokens.

        Returns a dict with:
          - ``users``: lookup table keyed by origin id (k_id)
          - ``groups``: lookup table keyed by group descriptor
        """
        logger.info("Discovering organisation users...")
        users_lookup = self.get_all_users()

        logger.info("Discovering organisation groups...")
        groups_lookup = self.get_all_groups()

        logger.info("Resolving user - group memberships...")
        users_lookup = self.enrich_users_with_memberships(users_lookup, groups_lookup)

        logger.info("Discovering PAT tokens...")
        users_lookup = self.enrich_users_with_pat_tokens(users_lookup)

        return {
            "users": users_lookup,
            "groups": groups_lookup,
        }


# ---------------------------------------------------------------------- #
#  User reference normalization (standalone function)
# ---------------------------------------------------------------------- #

# Keys that contain inline user identity blocks we want to collapse
_USER_REF_KEYS = frozenset([
    "createdBy",
    "authoredBy",
    "requestedBy",
    "requestedFor",
    "modifiedBy",
])


def normalise_user_references(obj):
    """
    Recursively walk *obj* and replace any inline user identity dict
    (under keys like ``createdBy``, ``authoredBy``, etc.) with just the
    user's ``id`` string - the k_id that maps into the ``users`` lookup
    table in the scan result.

    Objects without a recognisable ``id`` field are left untouched.
    """
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            value = obj[key]
            if key in _USER_REF_KEYS and isinstance(value, dict):
                user_id = value.get("id")
                if user_id:
                    obj[key] = user_id
                # else: leave as-is (e.g. system account with no usable id)
            else:
                normalise_user_references(value)
    elif isinstance(obj, list):
        for item in obj:
            normalise_user_references(item)
    return obj

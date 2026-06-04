#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

"""
Identity Resolution Integration Service

Provides optional integration with Laughing Lamp for resolving cloud identities
(Entra ID, GCP) associated with Azure DevOps service connections, variable groups,
and secure files.

This integration is fault-tolerant: if Laughing Lamp is not installed or
encounters errors, the scan will continue without identity resolution data.
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class IdentityResolutionService:
    def __init__(self, enabled: bool = True):
        """Initialize the identity resolution service.
        
        Args:
            enabled: Whether to attempt identity resolution. Set to False to
                    skip resolution entirely.
        """
        self.enabled = enabled
        self._resolver = None
        self._available = False
        
        if enabled:
            self._try_initialize()
    
    def _try_initialize(self) -> None:
        """Attempt to initialize Laughing Lamp integration."""
        try:
            # Try to import Laughing Lamp
            from laughing_lamp.integrations.azdo import (
                AzureDevOpsScanner,
                IdentityResolver,
                ReportGenerator,
            )
            
            self._scanner_cls = AzureDevOpsScanner
            self._resolver_cls = IdentityResolver
            self._report_gen_cls = ReportGenerator
            self._available = True
            logger.info("Laughing Lamp identity resolution is available")
            
        except ImportError as e:
            logger.info(
                f"Laughing Lamp not installed - identity resolution disabled. "
            )
            self._available = False
        except Exception as e:
            logger.warning(
                f"Failed to initialize Laughing Lamp: {e}. "
                f"Identity resolution will be disabled."
            )
            self._available = False
    
    @property
    def is_available(self) -> bool:
        """Check if identity resolution is available."""
        return self.enabled and self._available
    
    def resolve_identities(
        self,
        scan_result: Dict[str, Any],
        resolve: bool = True,
    ) -> Dict[str, Any]:

        if not self.is_available:
            logger.debug("Identity resolution not available, returning original scan")
            return scan_result
        
        try:
            return self._do_resolve(scan_result, resolve)
        except Exception as e:
            logger.error(
                f"Identity resolution failed: {e}. "
                f"Returning scan result without identity data.",
                exc_info=True
            )
            # Add metadata indicating resolution was attempted but failed
            scan_result["_identity_resolution"] = {
                "status": "error",
                "error": str(e),
                "message": "Identity resolution failed but scan completed successfully"
            }
            return scan_result
    
    def _do_resolve(
        self,
        scan_result: Dict[str, Any],
        resolve: bool
    ) -> Dict[str, Any]:

        from laughing_lamp.integrations.azdo import (
            AzureDevOpsScanner,
            IdentityResolver,
        )
        
        # Create a scanner that works with in-memory data instead of file
        scanner = _InMemoryScanner(scan_result)
        
        # Extract identities
        identities = scanner.extract_all_identities()
        
        total_extracted = sum(len(v) for v in identities.values())
        logger.info(f"Extracted {total_extracted} identities from scan")
        
        # Resolve if requested
        if resolve:
            resolver = IdentityResolver()
            for category, identity_list in identities.items():
                resolvable = [i for i in identity_list if i.can_resolve]
                if resolvable:
                    logger.info(f"Resolving {len(resolvable)} identities in {category}")
                    try:
                        resolver.resolve_all(resolvable)
                    except Exception as e:
                        logger.warning(
                            f"Failed to resolve {category} identities: {e}. Continuing..."
                        )
        
        # Enrich the scan result with resolution data
        enriched = self._enrich_scan_result(scan_result, identities)
        
        # Add success metadata
        enriched["_identity_resolution"] = {
            "status": "success",
            "total_extracted": total_extracted,
            "resolvable": sum(
                1 for cat in identities.values() 
                for i in cat if i.can_resolve
            ),
            "resolved": sum(
                1 for cat in identities.values() 
                for i in cat if i.resolution_result is not None
            ),
        }
        
        return enriched
    
    def _enrich_scan_result(
        self,
        scan_result: Dict[str, Any],
        identities: Dict[str, list]
    ) -> Dict[str, Any]:
        """Enrich the scan result with identity resolution data."""
        import copy
        enriched = copy.deepcopy(scan_result)
        
        # Build identity lookup by resource type and ID
        identity_map = {}
        for category, identity_list in identities.items():
            for identity in identity_list:
                key = (identity.resource_type, identity.resource_id)
                identity_map[key] = identity
        
        protected_resources = enriched.get("protected_resources", {})
        
        # Enrich endpoints
        self._enrich_resource_type(
            protected_resources.get("endpoint", {}).get("protected_resources", []),
            identity_map,
            "endpoint"
        )
        
        # Enrich variable groups
        self._enrich_resource_type(
            protected_resources.get("variablegroup", {}).get("protected_resources", []),
            identity_map,
            "variablegroup"
        )
        
        # Enrich secure files
        self._enrich_resource_type(
            protected_resources.get("securefile", {}).get("protected_resources", []),
            identity_map,
            "securefile"
        )
        
        return enriched
    
    def _enrich_resource_type(
        self,
        resources: list,
        identity_map: dict,
        resource_type: str
    ) -> None:
        """Enrich a list of resources with identity resolution data."""
        for wrapper in resources:
            resource = wrapper.get("resource", {})
            resource_id = str(resource.get("id", ""))
            
            key = (resource_type, resource_id)
            if key in identity_map:
                identity = identity_map[key]
                resource["_identity_resolution"] = self._build_resolution_block(identity)
    
    def _build_resolution_block(self, identity) -> Dict[str, Any]:
        """Build the identity resolution block for a resource."""
        return {
            "extracted": {
                "tenant_type": identity.tenant_type,
                "tenant_id": identity.tenant_id,
                "identity_id": identity.identity_id,
                "tenant_scope": identity.tenant_scope,
                "logic_container_selector": identity.logic_container_selector,
            },
            "can_resolve": identity.can_resolve,
            "missing_fields": identity.missing_fields if not identity.can_resolve else [],
            "resolution_result": identity.resolution_result,
            "resolution_error": identity.resolution_error,
        }


class _InMemoryScanner:
    """Adapter to use AzureDevOpsScanner with in-memory data."""
    
    def __init__(self, scan_data: Dict[str, Any]):
        self.scan_data = scan_data
        # Import the extraction logic from laughing_lamp
        from laughing_lamp.integrations.azdo import AzureDevOpsScanner
        
        # Create a scanner instance and inject our data
        self._scanner = object.__new__(AzureDevOpsScanner)
        self._scanner.scan_data = scan_data
        self._scanner.scan_file_path = None  # Not from file
    
    def extract_all_identities(self):
        """Delegate to the real scanner's extraction logic."""
        return self._scanner.extract_all_identities()


# Convenience function for direct use
def resolve_scan_identities(
    scan_result: Dict[str, Any],
    enabled: bool = True,
    resolve: bool = True,
) -> Dict[str, Any]:
    """Convenience function to resolve identities in a scan result.
    
    Args:
        scan_result: The complete scan result dictionary
        enabled: Whether identity resolution is enabled
        resolve: Whether to actually resolve (call cloud APIs) or just extract
    
    Returns:
        The enriched scan result, or original if resolution unavailable/fails
    """
    service = IdentityResolutionService(enabled=enabled)
    return service.resolve_identities(scan_result, resolve=resolve)

#!/usr/bin/env python3
#
# Copyright (c) 2024 The Yaook Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
OVN Router Eviction Script

Evicts Logical Router Ports (LRPs) from a specified OVN chassis node by
migrating them to other active chassis nodes.

This script was created as a standalone version based on:
https://gitlab.com/yaook/operator/-/blob/a2fe020de96bfbc01841e6a010701ebab66e4c39/yaook/op/neutron_ovn/eviction.py

By default the script runs in dry-run mode: it only logs what would be done
without making any changes. Pass --apply to actually apply changes to the OVN DB.

Usage:
    # Dry-run (default): only log planned changes
    # --node-name can be either a chassis name or hostname
    python evict_ovn_router.py --node-name chassis-01 \
        --nb-db tcp:192.168.1.1:6641 \
        --sb-db tcp:192.168.1.1:6642

    # Apply changes for real
    python evict_ovn_router.py --node-name chassis-01 \
        --nb-db tcp:192.168.1.1:6641 \
        --sb-db tcp:192.168.1.1:6642 --apply
    # IPv6 example:
    python evict_ovn_router.py --node-name az2-snat-1 \
        --nb-db tcp:[2a0a:db80:1042:0:1::1]:6641 \
        --sb-db tcp:[2a0a:db80:1042:0:1::1]:6642
"""
import argparse
import dataclasses
import itertools
import logging
import sys
import time
import typing
import fnmatch

from collections import defaultdict

from ovsdbapp.schema.ovn_northbound import impl_idl as nb_impl
from ovsdbapp.schema.ovn_southbound import impl_idl as sb_impl
from ovsdbapp.backend.ovs_idl.idlutils import RowNotFound
from ovsdbapp.backend.ovs_idl import connection


DEFAULT_TARGET_CHASSIS = "*"
DEFAULT_DB_COMMIT_INTERVAL = 10
DEFAULT_DB_CONNECTION_TIMEOUT = 150
DEFAULT_DB_PROBE_INTERVAL = 0


@dataclasses.dataclass(frozen=True)
class EvictionStatus:
    migrated_lrps: int
    unmigratable_lrps: int
    unhandleable_gateway: bool


class DatabaseError(Exception):
    pass


def connect_to_ovn_dbs(
    nb_db: str,
    sb_db: str,
    db_connection_timeout: int,
    db_probe_interval: int,
    logger: logging.Logger,
) -> tuple[nb_impl.OvnNbApiIdlImpl, sb_impl.OvnSbApiIdlImpl]:
    """Connect to OVN Northbound and Southbound databases."""
    logger.info("Connecting to the OVN DBs")
    try:
        nb_idl = connection.OvsdbIdl.from_server(
            nb_db, "OVN_Northbound", leader_only=False,
            probe_interval=db_probe_interval
        )
        nb_conn = connection.Connection(idl=nb_idl, timeout=db_connection_timeout)
        nb_api = nb_impl.OvnNbApiIdlImpl(nb_conn)

        sb_idl = connection.OvsdbIdl.from_server(
            sb_db, "OVN_Southbound", leader_only=False,
            probe_interval=db_probe_interval
        )
        sb_conn = connection.Connection(idl=sb_idl, timeout=db_connection_timeout)
        sb_api = sb_impl.OvnSbApiIdlImpl(sb_conn)
    except Exception as e:
        raise DatabaseError(
            f"Cannot connect to OVS DBs: {e}"
        ) from e
    logger.info("Connected to the OVN DBs")
    return (nb_api, sb_api)


def resolve_node_name_to_chassis_name(
    sb_api: sb_impl.OvnSbApiIdlImpl,
    node_name: str,
    logger: logging.Logger,
) -> str:
    """
    Resolve node name to chassis name (UUID).

    First checks if the passed value is a chassis name.
    If not found, checks if it's a hostname and resolves it to a chassis name.
    """
    logger.info(f"Resolving node name '{node_name}' to chassis name")

    # First, check if the passed value is already a chassis name
    chassis_rows = sb_api.db_find_rows(
        "Chassis",
        ('name', '=', node_name)
    ).execute()

    if chassis_rows:
        if len(chassis_rows) > 1:
            chassis_names = [row.name for row in chassis_rows]
            raise ValueError(
                f"Multiple chassis found with name '{node_name}': {chassis_names}. "
                "Cannot determine which chassis to evict."
            )
        chassis_name = chassis_rows[0].name
        logger.info(f"Found chassis name '{chassis_name}' directly")
        return chassis_name

    # If not found as chassis name, try resolving as hostname
    logger.info(f"Node name '{node_name}' not found as chassis name, trying as hostname")
    chassis_rows = sb_api.db_find_rows(
        "Chassis",
        ('hostname', '=', node_name)
    ).execute()

    if not chassis_rows:
        raise ValueError(
            f"Chassis with name or hostname '{node_name}' not found in OVN Southbound database"
        )

    if len(chassis_rows) > 1:
        chassis_names = [row.name for row in chassis_rows]
        raise ValueError(
            f"Multiple chassis found with hostname '{node_name}': {chassis_names}. "
            "Cannot determine which chassis to evict."
        )

    chassis_name = chassis_rows[0].name
    logger.info(f"Resolved hostname '{node_name}' to chassis name '{chassis_name}'")
    return chassis_name


def get_active_chassis(
    nb_api: nb_impl.OvnNbApiIdlImpl,
    sb_api: sb_impl.OvnSbApiIdlImpl,
    target_chassis: str,
    logger: logging.Logger,
) -> tuple[list[str], list[str]]:
    """Get list of all chassis and active chassis."""
    all_chassis: list[str] = []
    active_chassis: list[str] = []

    rows = nb_api.db_list_rows("NB_Global").execute()
    nb_cfg_ts_epoch = 0
    for row in rows:
        nb_cfg_ts_epoch = row.nb_cfg_timestamp

    if nb_cfg_ts_epoch == 0:
        raise Exception("nb_cfg_timestamp is wrong in the NB_Global. Aborting")

    chassis_rows = sb_api.db_find_rows(
        "Chassis",
        ('other_config', '=', {'ovn-cms-options': 'enable-chassis-as-gw'})
    ).execute()

    def should_include(chassis_name: str) -> bool:
        if fnmatch.fnmatchcase(chassis_name, target_chassis):
            return True
        logger.info(
            f"Chassis {chassis_name} isn't part of the target chassis, "
            "thus won't be considered for eviction"
        )
        return False

    for row in chassis_rows:
        if should_include(row.name):
            all_chassis.append(row.name)
            try:
                nb_cfg_timestamp = sb_api.db_get(
                    'Chassis_Private', row.name, 'nb_cfg_timestamp'
                ).execute()
                # 2 mins diff. is acceptable
                if nb_cfg_timestamp >= nb_cfg_ts_epoch - 120000:
                    active_chassis.append(row.name)
                else:
                    logger.warning(
                        f"`nb_cfg_timestamp` older than 2 mins for {row.name}, "
                        "not counting it as ACTIVE chassis"
                    )
            except RowNotFound:
                logger.warning(
                    f"`nb_cfg_timestamp` not found for {row.name} "
                    "in `Chassis_Private` table"
                )

    logger.info(f"Active chassis: {active_chassis}")
    logger.info(f"All chassis: {all_chassis}")
    return (all_chassis, active_chassis)


def fill_lrp_distribution(
    nb_api: nb_impl.OvnNbApiIdlImpl,
    all_chassis: list[str],
    current_dist: dict[str, dict[int, list[str]]],
    priorities: list[int],
    logger: logging.Logger,
) -> None:
    """Collect Logical Router Port distribution from OVN DB."""
    priority_set = set()
    current_dist.clear()
    current_dist.update({
        chassis: defaultdict(list)
        for chassis in all_chassis
    })

    logger.info("Collecting LRPs distribution from OVN DB")
    lrps = nb_api.db_find_rows(
        'Logical_Router_Port',
        ('gateway_chassis', '!=', [])
    ).execute()

    for lrp in lrps:
        for gc_row in lrp.gateway_chassis:
            priority_set.add(gc_row.priority)
            # ignore LRP entries pointing to dead chassis
            if gc_row.chassis_name in current_dist:
                current_dist[gc_row.chassis_name][gc_row.priority].append(lrp.name)

    priorities.clear()
    priorities.extend(sorted(priority_set))

    # Display distribution
    for priority in priorities:
        chassis_lrp_count = {}
        logger.info(f"Priority {priority}")
        for chassis in current_dist:
            chassis_lrp_count[chassis] = len(current_dist[chassis][priority])
            logger.info(f"  {chassis} has {chassis_lrp_count[chassis]} routers")

    logger.info("Collected LRPs distribution")


def search_for_chassis(
    priority: int,
    active_chassis: list[str],
    node_name: str,
    current_dist: dict[str, dict[int, list[str]]],
) -> list[str]:
    """Find candidate chassis sorted by LRP count (least loaded first)."""
    chassis_lrp_count = {}
    for chassis in active_chassis:
        if chassis != node_name:
            chassis_lrp_count[chassis] = len(current_dist[chassis][priority])
    return sorted(chassis_lrp_count, key=lambda c: chassis_lrp_count[c])


def update_priorities(
    old_chassis: str,
    new_chassis: str,
    lrp: str,
    prio_to_be_migrated: int,
    priorities: list[int],
    current_dist: dict[str, dict[int, list[str]]],
    updates: list[tuple[str, int, str, str]],
) -> bool:
    """Update priority mapping and queue migration."""
    # Check if we have the same LRP with another priority on the new gateway
    for prio in priorities:
        if lrp in current_dist[new_chassis][prio]:
            return False

    current_dist[old_chassis][prio_to_be_migrated].remove(lrp)
    current_dist[new_chassis][prio_to_be_migrated].append(lrp)
    updates.append((lrp, prio_to_be_migrated, old_chassis, new_chassis))
    return True


def evict_lrps(
    node_name: str,
    active_chassis: list[str],
    priorities: list[int],
    current_dist: dict[str, dict[int, list[str]]],
    updates: list[tuple[str, int, str, str]],
    logger: logging.Logger,
) -> None:
    """Plan eviction of LRPs from the target node."""
    if not current_dist:
        raise DatabaseError(
            "Couldn't determine active chassis. There might be a temporary issue "
            "in the connection to northbound database."
        )

    if node_name not in current_dist:
        logger.warning(
            f"Node name {node_name} is not listed as key in current distribution data. "
            f"Keys are {repr(list(current_dist.keys()))}"
        )
        return

    # If we have less than 5 chassis just remove priority of the LRP from
    # the current chassis
    if len(active_chassis) <= 5:
        for priority in priorities:
            lrp_list = current_dist[node_name][priority].copy()
            for lrp in lrp_list:
                current_dist[node_name][priority].remove(lrp)
                updates.append((lrp, priority, node_name, ""))
    else:
        for priority in priorities:
            lrp_list = current_dist[node_name][priority].copy()
            for lrp in lrp_list:
                candidate_chassis = search_for_chassis(
                    priority, active_chassis, node_name, current_dist
                )
                for chassis in candidate_chassis:
                    if update_priorities(
                        node_name, chassis, lrp, priority,
                        priorities, current_dist, updates
                    ):
                        break


def commit_updates(
    nb_api: nb_impl.OvnNbApiIdlImpl,
    updates: list[tuple[str, int, str, str]],
    sleep: float,
    logger: logging.Logger,
    dry_run: bool = True,
) -> bool:
    """Commit eviction updates to OVN database (or only log in dry-run)."""
    if dry_run:
        logger.info(f"[DRY-RUN] Would commit {len(updates)} updates to OVN DB")
        for lrp, priority, old_gc, new_gc in updates:
            if new_gc == "":
                logger.info(f"[DRY-RUN] Would remove LRP {lrp} from {old_gc}")
            else:
                logger.info(
                    f"[DRY-RUN] Would migrate priority {priority} for LRP {lrp} "
                    f"from {old_gc} to {new_gc}"
                )
        return True

    logger.info(f"Committing {len(updates)} updates to OVN DB")
    for _ in range(len(updates)):
        lrp, priority, old_gc, new_gc = updates.pop()
        try:
            with nb_api.transaction(check_error=True) as txn:
                if new_gc == "":
                    logger.info(f"Removing LRP {lrp} from {old_gc}")
                    txn.add(nb_api.lrp_del_gateway_chassis(lrp, old_gc))
                else:
                    logger.info(
                        f"Migrating priority {priority} for LRP {lrp} "
                        f"from {old_gc} to {new_gc}"
                    )
                    txn.add(nb_api.lrp_set_gateway_chassis(lrp, new_gc, priority))
                    txn.add(nb_api.lrp_del_gateway_chassis(lrp, old_gc))

            if priority < 5:
                # Higher priority (lower number) = less sleep time
                time.sleep(sleep / (6 - priority))
            else:
                time.sleep(sleep)
        except Exception as e:
            logger.error(f"Failed to commit updates. Error: {e}")
            return False
    return True


def lrps_on_chassis(
    nb_api: nb_impl.OvnNbApiIdlImpl,
    gateway_name: str,
) -> list:
    """Get list of LRPs on a specific chassis."""
    chassis_info = nb_api.db_find(
        "Gateway_Chassis", ("chassis_name", "=", gateway_name)
    ).execute()
    return chassis_info


def run_eviction_iteration(
    node_name: str,
    nb_api: nb_impl.OvnNbApiIdlImpl,
    sb_api: sb_impl.OvnSbApiIdlImpl,
    target_chassis: str,
    db_commit_interval: int,
    logger: logging.Logger,
    dry_run: bool = True,
) -> EvictionStatus:
    """Run a single eviction iteration."""
    unhandleable_gateway = True
    migrated_lrps = 0

    # Get active chassis
    all_chassis, active_chassis = get_active_chassis(
        nb_api, sb_api, target_chassis, logger
    )

    # Collect LRP distribution
    current_dist: dict[str, dict[int, list[str]]] = {}
    priorities: list[int] = []
    fill_lrp_distribution(nb_api, all_chassis, current_dist, priorities, logger)

    # Plan evictions
    updates: list[tuple[str, int, str, str]] = []
    evict_lrps(node_name, active_chassis, priorities, current_dist, updates, logger)

    migrated_lrps = len(updates)
    eviction_succeeded = commit_updates(
        nb_api, updates, db_commit_interval, logger, dry_run=dry_run
    )

    if dry_run:
        # In dry-run, we don't actually apply changes, so nothing failed
        unmigratable_lrps = 0
        unhandleable_gateway = False
        logger.info(
            f"Dry-run: would evict {migrated_lrps} LRPs from gateway chassis "
            f"{node_name}"
        )
    elif not eviction_succeeded:
        unmigratable_lrps = len(updates)
        logger.info(
            f"Failed to evict {len(updates)} LRPs out of {migrated_lrps} "
            f"from gateway chassis {node_name}"
        )
    else:
        unmigratable_lrps = len(updates)  # Remaining after successful commits
        unhandleable_gateway = False
        logger.info(
            f"Eviction of {migrated_lrps} LRPs from gateway chassis "
            f"{node_name} succeeded"
        )

    return EvictionStatus(
        migrated_lrps=migrated_lrps,
        unmigratable_lrps=unmigratable_lrps,
        unhandleable_gateway=unhandleable_gateway,
    )


def is_migration_done(
    nb_api: nb_impl.OvnNbApiIdlImpl,
    node_name: str,
    status: EvictionStatus,
) -> bool:
    """Check if migration is complete."""
    remaining_chassis = lrps_on_chassis(nb_api, node_name)
    return len(remaining_chassis) == 0 and not status.unhandleable_gateway


def run_eviction_loop(
    node_name: str,
    nb_db: str,
    sb_db: str,
    target_chassis: str,
    db_commit_interval: int,
    db_connection_timeout: int,
    db_probe_interval: int,
    reason: str,
    logger: logging.Logger,
    dry_run: bool = True,
) -> int:
    """Main eviction loop."""
    if dry_run:
        logger.info(
            "DRY-RUN mode: no changes will be made. Use --apply to apply changes."
        )
    logger.info(
        f"Initiating eviction of node {node_name} with reason {reason}"
    )

    # Connect to databases
    nb_api, sb_api = connect_to_ovn_dbs(
        nb_db, sb_db, db_connection_timeout, db_probe_interval, logger
    )

    # Resolve node name to chassis name (UUID)
    # First checks if it's a chassis name, then falls back to hostname resolution
    chassis_name = resolve_node_name_to_chassis_name(sb_api, node_name, logger)
    
    try:
        for iteration in itertools.count():
            logger.info(f"Starting iteration {iteration}")

            # Run eviction iteration
            status = run_eviction_iteration(
                chassis_name, nb_api, sb_api, target_chassis,
                db_commit_interval, logger, dry_run=dry_run
            )

            logger.info(f"Iteration {iteration} complete: {status}")
            
            # In dry-run mode, exit after first iteration since we can't verify completion
            # (the database doesn't change, so we'd loop forever)
            if dry_run:
                logger.info(
                    "Dry-run complete. Use --apply to actually perform the eviction."
                )
                return 0

            # Check if done
            if is_migration_done(nb_api, chassis_name, status):
                logger.info("Eviction complete!")
                return 0

            # No sleep needed - commit_updates() already has sleeps between migrations
            # and we can check immediately after commits are done
    except KeyboardInterrupt:
        logger.info("Eviction interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Eviction failed: {e}", exc_info=True)
        return 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Evict OVN router ports from a chassis"
    )
    parser.add_argument(
        "--node-name",
        required=True,
        help="Chassis name or hostname of the chassis node to evict (will be resolved to chassis UUID)"
    )
    parser.add_argument(
        "--nb-db",
        required=True,
        help="OVN Northbound database connection string (e.g., tcp:192.168.1.1:6641 or tcp:[2a0a:db80:1042:0:1::1]:6641 for IPv6)"
    )
    parser.add_argument(
        "--sb-db",
        required=True,
        help="OVN Southbound database connection string (e.g., tcp:192.168.1.1:6642 or tcp:[2a0a:db80:1042:0:1::1]:6642 for IPv6)"
    )
    parser.add_argument(
        "--target-chassis",
        default=DEFAULT_TARGET_CHASSIS,
        help=f"Chassis name pattern to consider for migration (default: {DEFAULT_TARGET_CHASSIS})"
    )
    parser.add_argument(
        "--db-commit-interval",
        type=int,
        default=DEFAULT_DB_COMMIT_INTERVAL,
        help=f"Base sleep time between DB commits in seconds (default: {DEFAULT_DB_COMMIT_INTERVAL})"
    )
    parser.add_argument(
        "--db-connection-timeout",
        type=int,
        default=DEFAULT_DB_CONNECTION_TIMEOUT,
        help=f"Database connection timeout in seconds (default: {DEFAULT_DB_CONNECTION_TIMEOUT})"
    )
    parser.add_argument(
        "--db-probe-interval",
        type=int,
        default=DEFAULT_DB_PROBE_INTERVAL,
        help=f"Database probe interval (default: {DEFAULT_DB_PROBE_INTERVAL})"
    )
    parser.add_argument(
        "--reason",
        default="manual-eviction",
        help="Reason for eviction (default: manual-eviction)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to OVN DB (default is dry-run: only log what would be done)"
    )

    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    return run_eviction_loop(
        node_name=args.node_name,
        nb_db=args.nb_db,
        sb_db=args.sb_db,
        target_chassis=args.target_chassis,
        db_commit_interval=args.db_commit_interval,
        db_connection_timeout=args.db_connection_timeout,
        db_probe_interval=args.db_probe_interval,
        reason=args.reason,
        logger=logger,
        dry_run=not args.apply,
    )


if __name__ == "__main__":
    sys.exit(main())



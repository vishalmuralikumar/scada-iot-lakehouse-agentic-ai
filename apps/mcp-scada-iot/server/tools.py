"""
Tools module for the MCP server.

This module defines all the tools that the MCP server exposes to clients.

Current tools:
- health
- get_current_user
- get_sensor_summary
"""

import os

from databricks.sdk.service.sql import (
    StatementParameterListItem,
    StatementState,
)

from server import utils


def load_tools(mcp_server):
    """
    Register all MCP tools with the server.

    Args:
        mcp_server: FastMCP server instance.
    """

    # ============================================================
    # TOOL 1: MCP SERVER HEALTH CHECK
    # ============================================================

    @mcp_server.tool
    def health() -> dict:
        """
        Check the health of the MCP server and Databricks connection.

        Returns:
            dict containing the MCP server health status.
        """

        return {
            "status": "healthy",
            "message": (
                "Custom MCP Server is healthy and connected "
                "to Databricks Apps."
            ),
        }

    # ============================================================
    # TOOL 2: CURRENT AUTHENTICATED USER
    # ============================================================

    @mcp_server.tool
    def get_current_user() -> dict:
        """
        Get information about the current authenticated user.

        Returns:
            dict containing:
            - display_name
            - user_name
            - active
        """

        try:
            w = utils.get_user_authenticated_workspace_client()

            user = w.current_user.me()

            return {
                "display_name": user.display_name,
                "user_name": user.user_name,
                "active": user.active,
            }

        except Exception as e:
            return {
                "error": str(e),
                "message": "Failed to retrieve user information",
            }

    # ============================================================
    # TOOL 3: SCADA SENSOR SUMMARY
    # ============================================================

    @mcp_server.tool
    def get_sensor_summary(sensor_id: int) -> dict:
        """
        Retrieve aggregated analytics for a specific SCADA sensor.

        Use this tool when a user asks about:
        - total readings for a sensor
        - average sensor value
        - minimum sensor value
        - maximum sensor value
        - measurement unit
        - general sensor summary

        Data source:
            workspace.gold.scada_lakeflow_gold

        Args:
            sensor_id:
                SCADA sensor identifier.

        Returns:
            dict containing:
            - sensor_id
            - unit
            - reading_count
            - avg_value
            - min_value
            - max_value
        """

        try:
            # ----------------------------------------------------
            # Get SQL Warehouse ID from Databricks App resource
            # ----------------------------------------------------

            warehouse_id = os.getenv("WAREHOUSE_ID")

            if not warehouse_id:
                return {
                    "error": "WAREHOUSE_ID is not configured",
                    "sensor_id": sensor_id,
                    "message": (
                        "The SQL Warehouse resource is not available "
                        "to the MCP application."
                    ),
                }

            # ----------------------------------------------------
            # Get user-authenticated Databricks client
            # ----------------------------------------------------

            w = utils.get_user_authenticated_workspace_client()

            # ----------------------------------------------------
            # Execute parameterized SQL query
            # ----------------------------------------------------

            response = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement="""
                    SELECT
                        id,
                        unit,
                        reading_count,
                        avg_value,
                        min_value,
                        max_value
                    FROM workspace.gold.scada_lakeflow_gold
                    WHERE id = :sensor_id
                    LIMIT 1
                """,
                parameters=[
                    StatementParameterListItem(
                        name="sensor_id",
                        value=str(sensor_id),
                        type="INT",
                    )
                ],
                wait_timeout="50s",
            )

            # ----------------------------------------------------
            # Check SQL execution status
            # ----------------------------------------------------

            if response.status is None:
                return {
                    "error": "SQL statement returned no status",
                    "sensor_id": sensor_id,
                }

            if response.status.state != StatementState.SUCCEEDED:
                state = (
                    response.status.state.value
                    if response.status.state
                    else "UNKNOWN"
                )

                error_message = "SQL query did not complete successfully."

                if response.status.error:
                    error_message = response.status.error.message

                return {
                    "error": error_message,
                    "state": state,
                    "sensor_id": sensor_id,
                }

            # ----------------------------------------------------
            # Extract SQL result
            # ----------------------------------------------------

            if response.result is None:
                return {
                    "found": False,
                    "sensor_id": sensor_id,
                    "message": "SQL query returned no result.",
                }

            rows = response.result.data_array

            if not rows:
                return {
                    "found": False,
                    "sensor_id": sensor_id,
                    "message": (
                        "Sensor was not found in the Gold "
                        "SCADA analytics table."
                    ),
                }

            row = rows[0]

            # ----------------------------------------------------
            # Return structured MCP response
            # ----------------------------------------------------

            return {
                "found": True,
                "sensor_id": int(row[0]),
                "unit": row[1],
                "reading_count": (
                    int(row[2])
                    if row[2] is not None
                    else None
                ),
                "avg_value": (
                    float(row[3])
                    if row[3] is not None
                    else None
                ),
                "min_value": (
                    float(row[4])
                    if row[4] is not None
                    else None
                ),
                "max_value": (
                    float(row[5])
                    if row[5] is not None
                    else None
                ),
            }

        except Exception as e:
            return {
                "error": str(e),
                "sensor_id": sensor_id,
                "message": (
                    "Failed to retrieve SCADA sensor summary."
                ),
            }

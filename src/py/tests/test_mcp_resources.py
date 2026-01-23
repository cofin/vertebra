import pytest
from vertebra.mcp.resources import mcp_resource
from vertebra.mcp.registry import ProcedureRegistry

def test_mcp_resource_registration():
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_resource("users://{user_id}/profile", name="User Profile")
    def get_user_profile(user_id: int):
        """Get user profile."""
        return {"id": user_id, "name": "Test User"}

    resource = registry.get_resource("users://{user_id}/profile")
    assert resource is not None
    assert resource.name == "User Profile"
    assert resource.description == "Get user profile."
    assert resource.handler == get_user_profile

def test_mcp_resource_list():
    registry = ProcedureRegistry.get_instance()
    registry.clear()

    @mcp_resource("config://settings")
    def get_settings():
        pass

    resources = registry.list_resources()
    assert len(resources) == 1
    assert resources[0].uri_template == "config://settings"

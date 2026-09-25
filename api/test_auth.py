import pytest
from fastapi.testclient import TestClient
from main import app
from shared.db import init_db, get_database_url


@pytest.fixture(scope="session", autouse=True)
def setup_db():
    init_db()


@pytest.fixture
def client():
    return TestClient(app)


class TestAuth:
    def test_signup_success(self, client):
        resp = client.post("/auth/signup", json={
            "username": "testuser1",
            "password": "password123"
        })
        assert resp.status_code == 200
        assert resp.json()["username"] == "testuser1"

    def test_signup_duplicate_username(self, client):
        client.post("/auth/signup", json={
            "username": "testuser2",
            "password": "password123"
        })
        resp = client.post("/auth/signup", json={
            "username": "testuser2",
            "password": "password456"
        })
        assert resp.status_code == 400
        assert "already exists" in resp.json()["detail"].lower()

    def test_signup_password_too_short(self, client):
        resp = client.post("/auth/signup", json={
            "username": "testuser3",
            "password": "short"
        })
        assert resp.status_code == 400
        assert "too short" in resp.json()["detail"].lower()

    def test_login_success(self, client):
        client.post("/auth/signup", json={
            "username": "testuser4",
            "password": "password123"
        })
        resp = client.post("/auth/login", json={
            "username": "testuser4",
            "password": "password123"
        })
        assert resp.status_code == 200
        assert "access_token" in resp.json()
        assert resp.json()["token_type"] == "bearer"

    def test_login_invalid_password(self, client):
        client.post("/auth/signup", json={
            "username": "testuser5",
            "password": "password123"
        })
        resp = client.post("/auth/login", json={
            "username": "testuser5",
            "password": "wrongpassword"
        })
        assert resp.status_code == 401
        assert "invalid" in resp.json()["detail"].lower()

    def test_login_nonexistent_user(self, client):
        resp = client.post("/auth/login", json={
            "username": "nonexistent",
            "password": "password123"
        })
        assert resp.status_code == 401

    def test_get_me_with_token(self, client):
        signup = client.post("/auth/signup", json={
            "username": "testuser6",
            "password": "password123"
        })
        user_id = signup.json()["id"]

        login = client.post("/auth/login", json={
            "username": "testuser6",
            "password": "password123"
        })
        token = login.json()["access_token"]

        resp = client.get("/auth/me", headers={
            "Authorization": f"Bearer {token}"
        })
        assert resp.status_code == 200
        assert resp.json()["id"] == user_id

    def test_get_me_without_token(self, client):
        resp = client.get("/auth/me")
        assert resp.status_code == 401
        assert "invalid or missing token" in resp.json()["detail"].lower()

    def test_get_me_invalid_token(self, client):
        resp = client.get("/auth/me", headers={
            "Authorization": "Bearer invalid.token.here"
        })
        assert resp.status_code == 401


class TestWorkflowProtection:
    def test_list_workflows_requires_auth(self, client):
        resp = client.get("/workflows")
        assert resp.status_code == 401

    def test_list_workflows_with_token(self, client):
        client.post("/auth/signup", json={
            "username": "wfuser1",
            "password": "password123"
        })
        login = client.post("/auth/login", json={
            "username": "wfuser1",
            "password": "password123"
        })
        token = login.json()["access_token"]

        resp = client.get("/workflows", headers={
            "Authorization": f"Bearer {token}"
        })
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_submit_workflow_requires_auth(self, client):
        resp = client.post("/workflows", json={
            "name": "test",
            "tasks": [{"id": "task1", "command": "cmd"}]
        })
        assert resp.status_code == 401

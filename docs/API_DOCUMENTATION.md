# API DOCUMENTATION

### 1. User

#### 1.1 Create Team

```bash
curl -X POST http://localhost:8080/api/v1/teams \
  -H "Content-Type: application/json" \
  -d '{
    "team_name": "My Team",
    "slug": "my-team",
    "description": "My Team",
    "org_name": "My-Organization"
  }'

Note: Only `team_name` and `org_name` are required. `slug`, `description`, and `color` are optional.
```

#### 1.2 Create User

```bash
curl -X POST http://localhost:8080/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "name": "John Doe",
    "role": "admin",
    "password": "securePassword123",
    "teamIds": [1]
  }'
```


#### 1.3 Login

```bash
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "securePassword123"
  }'
```

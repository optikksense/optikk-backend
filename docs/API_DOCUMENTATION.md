# API DOCUMENTATION

### 1. User

#### 1.1 Create Team

```bash
curl -X POST http://localhost:8080/api/teams \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Team",
    "slug": "my-team",
    "description": "My Team",
    "color": "#3B82F6",
    "orgName": "My Organization"
  }'
```

#### 1.2 Create User

```bash
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "name": "John Doe",
    "role": "admin",
    "password": "securePassword123",
    "teamIds": [1]
  }'
```


#### 1.1 Login

```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "securePassword123"
  }'
```

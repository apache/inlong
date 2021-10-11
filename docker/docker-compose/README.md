#### InLong Standalone Using Docker Compose
Deploy all InLong module by Docker Compose, it's only available for development.

Requirements:
- [Docker](https://docs.docker.com/engine/install/) 19.03.1+
- Docker Compose 1.29.2+

##### Deploy
```
docker-compose up -d
```

##### Use InLong
After all containers run successfully, you can access `http://localhost` with default account:
```
User: admin
Password: inlong
```

##### Destroy
```
docker-compose down
```
# TrinityLake Gravitino Iceberg Rest Server 

Run a TrinityLake-backed Iceberg REST server in a Docker container.

## Build the Docker Image

When making changes to the local files and test them out, you can build the image locally:

```bash
# Build the project from trinitylake root directory
./gradlew :trinitylake-spark:build

# Rebuild the docker image
docker image rm -f trinitylake/trinitylake-gravitino-iceberg-rest-server && docker build -t trinitylake/trinitylake-gravitino-iceberg-rest-server -f docker/trinitylake-gravitino-iceberg-rest-server/Dockerfile .
```

## Run the Docker Container

```bash
# Run the Gravitino IRC Server container with port mapping
docker run -d -p 9001:9001 --name trinitylake-gravitino-iceberg-rest-server trinitylake/trinitylake-gravitino-iceberg-rest-server
```

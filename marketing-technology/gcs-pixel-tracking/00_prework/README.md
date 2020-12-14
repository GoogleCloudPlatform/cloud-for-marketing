# Prework (optional)

## Files
### load_no_create.sh:
This file
1. Downloads the file from gs://[YOUR_PREFIX]-gcs-pixel-tracking
2. Uses that file to attack through Vegeta

Note: Targets.txt is about 12MB but the download should not take more than 1 sec from GCS to Pods. This is faster than creating a file every time we create a pod

It will be called using ENTRYPOINT in the Docker file and its parameters will be passed using args from the Deployments. Passed parameters are:
- V_DURATION: This is the time that the attack will last for
- V_RATE: This is the amount of requests sent per second
- TARGETS_FILE: This is the location of the file container the urls to attack similar to "GET http://LB_IP_OR_DOMAIN/pixel.png?params"

### Dockerfile
The Dockerfile will help us create the container that we need. It will use golang base image and we will:
1. Install Vegeta
2. Launch the load script through ENTRYPOINT so we will be able to send it some parameters through the replication controller

To make it available through your gcr.io:
```
docker build --no-cache -t USERNAME/vegeta docker
docker tag matthieum/vegeta gcr.io/CLOUD_REPOSITORY/vegeta
gcloud docker -- push gcr.io/CLOUD_REPOSITORY/vegeta
```

The one that we make available for this tutorial is available at gcr.io/cloud-solutions-images/vegeta

If you want to test it Locally
```
docker build --no-cache -t vegeta docker
docker run vegeta 1s 1000 gcs-pixel-tracking/targets.txt
```

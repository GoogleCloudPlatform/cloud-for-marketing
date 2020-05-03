# How to load test a serverless pixel tracking setup

This load testing environment was written specifically for a [serverless pixel tracking architecture solution paper](https://cloud.google.com/solutions/serverless-pixel-tracking) but the process can be reused for other purposes as well.

## Prework (Optional)
The [00_prework](/00_prework) folder contains the files that are used by Container Engine in order to launch the load testing. We included those files for information or for you to be able to customize the environment but to run this code, you do not need to changed anything here.

You can find the explanation in the [README](/00_prework/README.md) file in that folder

## Preparation
We need to do a few things before getting started:
1. Create a target file with the list of urls that Vegeta will use
2. Upload that file to a Google Cloud Storage bucket that offers great throughput to/from instances. It will be used later when Pods are created.

Note: There are different ways to use Vegeta. We could have created a random string using shell and continuously pass it to Vegeta but having a set makes it easier to have a fair and heterogenous repartition of urls per second.

The [01_prep](/01_prep) folder contains material to create a txt file with URLs to tests:

- [create_targets.sh](/01_prep/create_targets.sh) : This file creates a txt file of X urls
- [targets_sample.txt](/01_prep/targets_sample.txt) is an example of 10 sample urls create with the below command line
- [data/domains.txt](/01_prep/data/domains.txt) contains a list of fictive domains
- [data/events.txt](/01_prep/data/events.txt) contains a list of possible events that the user might have done
- [data/page_names.txt](/01_prep/data/page_names.txt) contains a list of pages that the user is visiting
- [data/products.txt](/01_prep/data/products.txt) contains a list of products that the use might have put in their cart

The url created is of the form
http://YOUR_LB_IP_OR_DOMAIN_NAME/pixel.png?uid=CUSTOMER_ID&pn=PAGE_NAME&purl=PAGE_URL&e=EVENT&pr=PRODUCT1[;PRODUCT2;...;PRODUCTN]

### Create the test URLs
If you are happy to reuse the combination that we set for you, you only need to create testing URLs. The YOUR_LB_IP_OR_DOMAIN_NAME is the IP of your load balancer or can also be your domain name if you attached a domain to it (which you should for a production environment)

```
bash create_targets.sh YOUR_LB_IP_OR_DOMAIN_NAME NB_URLS OUTPUT_FILE
```

### Save targets URL to your bucket
We now need to upload this file to a bucket accessible and make it public so it can easily be downloaded from our pods.
For more details on file ACLs, refer to our [Google Cloud Storage ACL documentation](https://cloud.google.com/storage/docs/gsutil/commands/acl)

```
gsutil mb gs://[YOUR_PREFIX]-load-testing
gsutil cp targets.txt gs://[YOUR_PREFIX]-load-testing
gsutil acl ch -u AllUsers:R gs://[YOUR_PREFIX]-gcs-load-testing/targets.txt
```

## Load test

### Create Kubernetes cluster
Containers need about:
- 0.4 max core from what we observe to 1000 qps
- 100Mi memory
We want 100000 > We need at least 12 4-vCPU machines (we want to keep the CPU for the group at a max around 80%).
It is always possible to grow our cluster during the load if there is not enough nodes
```
gcloud container clusters resize vegeta --size NUM_OF_NODES
```

As seen below, with 12 nodes, our CPU is about 80% which is quite high. A choice of 15 nodes seems good.
![GKE cluster CPUs](https://storage.googleapis.com/solutions-public-assets/pixel-tracking/readme-images/gke_cpu.png)

```
gcloud container clusters create vegeta --machine-type n1-standard-4 --num-nodes=15
kubectl config use-context gke_PROJECT-ID_ZONE_CLUSTER-NAME
```

### Attack

Creating our cluster to load will use various files

load_scaleup.sh which loads progressively
- Start with one pod that does 1000/s
- Increase pod size progressively up to 100K qps

vegeta-deployment.yaml which creates a Deployment file
- Use a grc.io file
- command will call load.sh NB_URLS YOUR_LB_IP_OR_DOMAIN_NAME DURATION RATE
- Set the pod CPU limit to 0.4

With the args and ENTRYPOINT, it will be equivalent to do "bash load_no_create.sh YOUR_LB_IP_OR_DOMAIN_NAME 30s 1000 load-testing/targets.txt"

Do not forget to replace in the file [YOUR_PREFIX] with the prefix you used when created your bucket

### Attack process
Create our replication controller which will help us scale the load and make sure that we have one pod running

```
kubectl create -f vegeta-deployment.yaml
kubectl get pods
```

You should see something similar to this:
![GKE pods](https://storage.googleapis.com/solutions-public-assets/pixel-tracking/readme-images/get_pod.png)
If it says running, then we can start scaling. We can see over time, the amount of request increasing to reach a steady 100K qps

```
bash load_scaleup.sh
```

We can follow the increasing load of our pixel across time. You can find an example of load monitoring below using Stackdriver.
![GKE cluster CPUs](https://storage.googleapis.com/solutions-public-assets/pixel-tracking/readme-images/monitoring.png)

It is a best practive for load testing to increase progressively the load in order to prevent DDoS protection to kick in. In the end, we should have 100 pods created. This is available through your Kubernetes UI.

Please note that this test might incur important costs as mentioned in our tutorial.

![GKE pods](https://storage.googleapis.com/solutions-public-assets/pixel-tracking/readme-images/100_pods.png)

## Disclaimer
This is not an official Google product

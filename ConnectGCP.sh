#!/bin/sh
#Shell script to manage GCP login



	ZONE="us-west1-b"
	INSTANCE_NAME="my-fastai-instance"
	
echo "Pick an option:
	1. Start GCP instance (Do this before trying to connect)
	2. Stop GCP instance (Do this after finishing work)
	3. Connect to GCP instance
	"
read opt

if [[ ${opt} == "1" ]]
then
	gcloud compute instances start my-fastai-instance
elif [[ ${opt} == "2" ]]
then
	gcloud compute instances stop my-fastai-instance
elif [[ ${opt} == "3" ]]
then
	gcloud compute ssh --zone=$ZONE jupyter@$INSTANCE_NAME -- -L 8080:localhost:8080
else
	echo "Invalid option was chosen, try again."
fi

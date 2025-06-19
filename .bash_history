gcloud config set project bright-voltage-462902-s0
gcloud services enable container.googleapis.com
cd terraform
mkdir main.tf
terraform init 
terraform init
terraform plan
diomedes3737@cloudshell:~/terraform$ terraform plan
╷
│ Error: Invalid function argument
│ 
│   on main.tf line 6, in provider "google":
│    6:   credentials = file("datahub-key.json")
│     ├────────────────
│     │ while calling file(path)
│ 
│ Invalid value for "path" parameter: no file exists at "datahub-key.json"; this function works only with files that are distributed as part of the configuration source code, so
│ if this file will be created by a resource in this configuration you must instead obtain this result from an attribute of that resource.
╵
╷
│ Error: Invalid function argument
│ 
│   on main.tf line 12, in provider "google-beta":
│   12:   credentials = file("datahub-key.json")
│     ├────────────────
│     │ while calling file(path)
│ 
│ Invalid value for "path" parameter: no file exists at "datahub-key.json"; this function works only with files that are distributed as part of the configuration source code, so
│ if this file will be created by a resource in this configuration you must instead obtain this result from an attribute of that resource.
╵eee
terraform plan
gcloud iam service-accounts keys create datahub-key.json   --iam-account=datahub@<your-project-id>.iam.gserviceaccount.com
gcloud iam service-accounts keys create datahub-key.json   --iam-account=datahub@<your-project-id>.iam.gserviceaccount.com
terraform plan -var="project_id=bright-voltage-462902-s0"
terraform apply  -var="project_id=bright-voltage-462902-s0"
cd terraform 
terraform plan
cd terraform
terraform plan 
terraform plan
terraform apply
gcloud container clusters get-credentials datawarehouse --zone asia-southeast1-a
gcloud config set project bright-voltage-462902-s0
gcloud container clusters get-credentials datawarehouse --zone asia-southeast1-a
terraform plan 
terraform apply
git init
cd ..
git init
git add.
git add .
git remote add origin https://github.com/Dungpham12345/cloud-based-dwh.git
git push -u origin master
git init
git add .
git init
git remote add origin https://github.com/Dungpham12345/cloud-based-dwh/tree/main
git add .
git remote remove origin
git remote add origin https://github.com/Dungpham12345/cloud-based-dwh.git
git remote remove origin
git remote add origin https://github.com/Dungpham12345/cloud-based-dwh/tree/main
git init
git add . 
git init
git remote add origin https://github.com/Dungpham12345/cloud-based-dwh.git
git add .
git remote remove origin
git remote add origin https://github.com/Dungpham12345/cloud-based-dwh.git
git init
git add .
git config --global user.name "Dungpham12345"
git config --global user.email "phammydng2003@gmail.com"
git push -u origin main
cd ~/cloud-based-dwh
cd terraform 
git init
git add .
git commit -m "Initial commit: Terraform DWH infrastructure"
git branch -M main
git remote add orgigin https://github.com/Dungpham12345/cloud-based-dwh.git
git push -u origin main
git push -u master
cd .. 
cd dags
cd .. 
cd terraform 
terraform init
terraform plan 
terraform applu
terraform apply
kubectl get svc
terraform init 
terraform plan
terraform apply
terraform plan
terraflrm apply
terraform apply
terraform plan
terraform apply
terraform plan
terraform apply
curl ifconfig.me
terraform plan
terraform apply
terrafor plan
terraform apply
gcloud sql instances patch postgres   --authorized-networks="$(curl -s ifconfig.me)/32"
cd dags 
gsutil storage cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
gcloud auth login
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud auth login
gcloud config set project bright-voltage-462902-s0
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud composer environments update datahub-composer   --location=asia-southeast1   --update-pypi-packages-from-file=requirements.txt
cd dgs
cd dags
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
cd dags 
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd terraform 
gcloud config set project bright-voltage-462902-s0
terraform init
terraform plan
gcloud container clusters get-credentials datawarehouse   --zone asia-southeast1-a   --project bright-voltage-462902-s0
gcloud container clusters list
kubectl get nodes
terraform apply
terraform apply 
cd .. 
cd dags
gcloud composer environments update datahub-composer   --location=asia-southeast1   --update-cloud-sql-instances=bright-voltage-462902-s0:asia-southeast1:datahub-postgres
gcloud sql instances patch postgres   --authorized-networks="$(curl -s ifconfig.me)/32"
psql -h 35.229.242.35 -U postgres -d postgres
gsutil cp etl gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags 
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags 
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
cd dags
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
cd dags
0
gsutil cp etl.py  gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp scd.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
cd dags
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd .. 
kubectl exec -it deployment/datahub-frontend -- printenv | grep AUTH
kubectl logs deployment/datahub-frontend
helm list -n default
kubectl get deployments -n default
kubectl get svc datahub-frontend-lb -n default
kubectl get pods -n default
kubectl exec -it datahub-datahub-frontend-7d858d6ccb-75c2g -n default -- printenv | grep AUTH
helm upgrade datahub datahub/datahub   --namespace default   --set global.datahub.auth.enabled=true   --set global.datahub.auth.systemDefaultUser.username=datahub   --set global.datahub.auth.systemDefaultUser.password=datahub 
helm repo add datahub https://helm.datahubproject.io/
helm repo update
helm upgrade datahub datahub/datahub   --namespace default   --set global.datahub.auth.enabled=true   --set global.datahub.auth.systemDefaultUser.username=datahub   --set global.datahub.auth.systemDefaultUser.password=datahub 
gcloud config set project bright-voltage-462902-s0
cd dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
kubectl get pods -n default
helm upgrade datahub datahub/datahub   --namespace default   --set global.datahub.auth.enabled=true   --set global.datahub.auth.systemDefaultUser.username=datahub   --set global.datahub.auth.systemDefaultUser.password=datahub   --no-hooks
kubectl get pods -n default
helm list -n default
kubectl logs pod/datahub-datahub-gms-6f8c994d8b-v5sqp -n default
kubectl logs pod/prerequisites-kafka-broker-0 -n default
kubectl get pods -n default | grep gms
kubectl logs deployment/datahub-datahub-gms -n default
gsutil cp scd.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project  bright-voltage-462902-s0
cd dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp scd.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
cd dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
gsutil cp scd.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags
gsutil cp scd.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gsutil cp scd2.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
gcloud config set project bright-voltage-462902-s0
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags 
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags
gsutil cp test.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags
gsutil cp scd2.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags 
gsutil cp scd2.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd dags
gsutil cp csv_etl.py gs://asia-southeast1-datahub-com-9e823cf2-bucket
gsutil cp csv_elt.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
cd ..
cd terraform 
terraform init
terraform plan
terraform apply
kubectl get pods -n default
kubectl logs deployment/datahub-datahub-gms
kubectl get svc,pods -l app=elasticsearch -n default
terraform apply -target=helm_release.datahub
kubectl get pods -n default
kubectl logs pod/prerequisites-kafka-broker-0
terraform plan
terraform apply
kubectl get jobs -n default
kubectl get pods -n default
kubectl logs pod/datahub-elasticsearch-setup-job-g4htm -n default
kubectl get pods -n default | grep elasticsearch
kubectl logs pod/elasticsearch-master-0 -n default
kubectl logs pod/prerequisites-kafka-broker-0 -n default
kubectl delete pod datahub-elasticsearch-setup-job-g4htm -n default
kubectl logs pod/prerequisites-kafka-broker-0 -n default
kubectl get pods -n default | grep elasticsearch
kubectl delete pod datahub-elasticsearch-setup-job-rqh7s -n default
kubectl delete pod datahub-elasticsearch-setup-job-sj8gb -n default
kubectl delete pod datahub-elasticsearch-setup-job-tkrkp -n default
terraform apply
cd dags 
gsutil cp scd2.py gs://asia-southeast1-datahub-com-9e823cf2-bucket/dags
ls
cd ..
ls
cd diomedes3737/
git init
ls -la
git filter-repo --force --path .kube/gke_gcloud_auth_plugin_cache --invert-paths 
git push -f origin main
ls
cd ..
ls
git remote add clean-origin https://github.com/Dungpham12345/etl_pipeline.git
ls
cd diomedes3737/
git remote add clean-origin https://github.com/Dungpham12345/etl_pipeline.git
git push -u clean-origin main
cd dags/
ls
sudo mv test.py etl_pipeline.py
ls
cd ..
ls
git push -u clean-origin main
ls cpu
lscpu
cat /proc/cpuinfo 
cat /proc/meminfo 
ifconfig 
clear
git push -u clean-origin master
git push -u clean-origin main
ls
cd dags
ls
cd ..
git mv dags/etl_pipeline.py etl_pipeline1.py
git mv dags/etl_pipeline.py dags/etl_pipeline1.py
git add dags/etl_pipeline.py
git mv dags/etl_pipeline.py dags/etl_pipeline1.py
git commit -m "Rename etl_pipeline.py to etl_pipeline1.py"
git push
git remote set-url origin https://github.com/Dungpham12345/Dwh_cloud_based
git remote -v
git remote set-url origin https://github.com/Dungpham12345/Dwh_cloud_based.git
git remote -v
git remote remove clean-origin
git remote -v
cat .gitmodules
ls -l
cd terraform/
ls
ls -la
cd ..
cat .gitmodules
ls -la
git add -f terraform/
git commit -m "Force add terraform folder"
git remote -v
ls
git ls-files | grep terraform
cat .gitignore | grep terraform
git add terraform/
git commit -m "Add terraform folder with config"
git add terraform/
git commit -m "Add terraform folder with config"
git push -f
ls -la terraform/
git check-ignore -v terraform/*
git rm --cached terraform
rm -rf .git/modules/terraform
git add terraform/
git commit -m "Add terraform folder as regular directory"
git push
git push --set-upstream origin main
git push --force origin main
git remote -v
ls
c dags/
cd dags/
ls
rm -rf .git
git init
git add .
git commit -m "Initial commit for new repository"
rm -rf .git
cd ..
rm -rf .git
git init

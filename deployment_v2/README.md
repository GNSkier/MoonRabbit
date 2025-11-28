# Deployment of Commodity Predictor Using Terraform
## Team Moon Rabbit: Helen Lin, Grant Nitta

### Objective:
Using terraform to automate the deployment of the GCP environment and machine learning (ML) model to predict commodity prices based on weather data. 

### Data Operations Pipeline Fit:
Terraform is a core component in the Continuous Integration/Continuous Deployment (CI/CD) portion of the Data Operations pipeline. It functions as an Infrastructure as Code (IaC) tool, solely responsible for automating the declarative provisioning of the underlying infrastructure that hosts the data pipeline resources. 
Configurations are made highly customizable and environment-agnostic through the use of `.tfvars` files. A key safety feature is the execution plan generated prior to deployment, which clearly details all intended changes. Once deployed, a state file tracks the live infrastructure, providing accurate information for planning subsequent updates and preventing configuration drift. Finally, Terraform’s ability to automatically generate a dependency graph ensures all cloud resources are created and configured in the precise order required.

### How to Deploy

1. Navigate to the `deployment_v2` directory: `cd ./deployment_v2`.
2. Run `terraform init`.
3. Copy Contents of `terraform_example.md` into a file called `terraform.tfvars`.
4. Update `terraform.tfvars` with your necessary GCP Configurations. 
5. Run `terraform init` in terminal and wait for successful run.
6. Run `terraform plan` in terminal and wait for successful run.
7. Run `terraform apply` in terminal and wait for successful run.
8. Check GCP for successful resource deployment.

### Evidence of Succesfull Deployment

- `./proof/terraform_init` Image of a successfull `terraform init` command.
- `./proof/terraform_init_output.txt` Output of a successfull `terraform init` command.
- `./proof/terraform_plan` Image of a successfull `terraform plan` command.
- `./proof/terraform_plan_output.txt` Output of a successfull `terraform plan` command.
- `./proof/terraform_apply` Image of a successfull `terraform apply` command.
- `./proof/terraform_apply_output.txt` Output of a successfull `terraform apply` command.


- `./terraform_gcp_bucket.png` Image of GCP buckets created by terraform.
- `./terraform_bigquery.png` Image of Bigquery resources created by terraform.
- `./terraform_cloud_run.png` Image of Cloud Run resources created by terraform. 
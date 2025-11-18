# Deployment of Commodity Predictor
## Team Moon Rabbit: Helen Lin, Grant Nitta

### Objective:
Using terraform to automate the deployment of the GCP environment and machine learning (ML) model to predict commodity prices based on weather data. 

### Data Operations Pipeline Fit:
Terraform is a core component in the Continuous Integration/Continuous Deployment (CI/CD) portion of the Data Operations pipeline. It functions as an Infrastructure as Code (IaC) tool, solely responsible for automating the declarative provisioning of the underlying infrastructure that hosts the data pipeline resources. 
Configurations are made highly customizable and environment-agnostic through the use of `.tfvars` files. A key safety feature is the execution plan generated prior to deployment, which clearly details all intended changes. Once deployed, a state file tracks the live infrastructure, providing accurate information for planning subsequent updates and preventing configuration drift. Finally, Terraform’s ability to automatically generate a dependency graph ensures all cloud resources are created and configured in the precise order required.

### Evidence of Succesfull Deployment
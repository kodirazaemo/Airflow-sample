variable "aws_region" {
  description = "AWS region for sample bronze storage and the optional warehouse."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name prefix for sample resources."
  type        = string
  default     = "airflow-sample"
}

variable "environment" {
  description = "Environment tag (dev/staging/prod). Sample default is dev."
  type        = string
  default     = "dev"
}

variable "bronze_bucket_name" {
  description = "Optional explicit S3 bucket name. Leave empty to let AWS generate a unique name."
  type        = string
  default     = ""
}

variable "enable_warehouse_db" {
  description = "If true, create a small RDS Postgres instance for a medallion warehouse."
  type        = bool
  default     = false
}

variable "db_instance_class" {
  description = "RDS instance class when enable_warehouse_db is true."
  type        = string
  default     = "db.t3.micro"
}

variable "db_name" {
  description = "Initial database name for the optional warehouse."
  type        = string
  default     = "airflow"
}

variable "db_username" {
  description = "Master username for the optional warehouse. Password is generated and not stored in git."
  type        = string
  default     = "airflow"
}

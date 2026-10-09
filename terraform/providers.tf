provider "aws" {
  region = var.aws_region

  # Sample only — do not put real credentials here. Use AWS_PROFILE,
  # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the environment, or
  # an assumed role. This repo does not ship secrets.
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

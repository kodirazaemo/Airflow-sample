locals {
  bucket_prefix = "${var.project_name}-${var.environment}-bronze-"
}

resource "aws_s3_bucket" "bronze" {
  bucket        = var.bronze_bucket_name != "" ? var.bronze_bucket_name : null
  bucket_prefix = var.bronze_bucket_name == "" ? local.bucket_prefix : null
  force_destroy = var.environment != "prod"

  tags = {
    Layer = "bronze"
  }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "random_password" "warehouse" {
  count   = var.enable_warehouse_db ? 1 : 0
  length  = 20
  special = false
}

resource "aws_db_instance" "warehouse" {
  count = var.enable_warehouse_db ? 1 : 0

  identifier     = "${var.project_name}-${var.environment}-warehouse"
  engine         = "postgres"
  engine_version = "15"
  instance_class = var.db_instance_class

  allocated_storage = 20
  storage_encrypted = true
  db_name           = var.db_name
  username          = var.db_username
  password          = random_password.warehouse[0].result

  skip_final_snapshot     = var.environment != "prod"
  backup_retention_period = var.environment == "prod" ? 7 : 0
  publicly_accessible     = false
  deletion_protection     = var.environment == "prod"

  tags = {
    Layer = "warehouse"
  }
}

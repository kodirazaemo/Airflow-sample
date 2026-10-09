output "bronze_bucket_id" {
  description = "S3 bucket for immutable bronze file storage."
  value       = aws_s3_bucket.bronze.id
}

output "bronze_bucket_arn" {
  description = "ARN of the bronze S3 bucket."
  value       = aws_s3_bucket.bronze.arn
}

output "warehouse_endpoint" {
  description = "RDS endpoint when enable_warehouse_db is true; otherwise null."
  value       = try(aws_db_instance.warehouse[0].address, null)
}

output "warehouse_port" {
  description = "RDS port when enable_warehouse_db is true; otherwise null."
  value       = try(aws_db_instance.warehouse[0].port, null)
}

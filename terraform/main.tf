provider "aws" {
  region = var.aws_region
}

terraform {
  backend "s3" {
    bucket = "terraform-backend-etl-ghiles"
    key    = "etl-ghiles.tfstate"
  }
}

# Data Lake s3 bucket 
resource "aws_s3_bucket" "data_lake" {
  bucket        = var.bucket_name
  force_destroy = true
}

# IAM role for Redshift to be able to use S3 

resource "aws_iam_role" "redshift_iam_role" {

  name = var.redshift_iam_role
  assume_role_policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "sts:AssumeRole"
        ],
        "Principal" : {
          "Service" : [
            "redshift.amazonaws.com"
          ]
        }
      }
    ]

  })
  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonS3FullAccess"]
}

# Redshift cluster open ports 
resource "aws_security_group" "allow_redshift_access" {

  name        = "etl-ghiles"
  description = "open the port 5439 to access the cluster"

  ingress {
    from_port   = var.dwh_port
    to_port     = var.dwh_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_redshift_cluster" "dwh_cluster" {

  cluster_identifier      = "${var.dwh_cluster_identifier}"
  database_name           = var.dwh_db
  master_username         = var.master_username
  master_password         = var.master_password
  node_type               = var.dwh_node_type
  cluster_type            = var.dwh_cluster_type
  port                    = var.dwh_port
  skip_final_snapshot     = true
  iam_roles               = ["${aws_iam_role.redshift_iam_role.arn}"]
  cluster_security_groups = ["${aws_security_group.allow_redshift_access.id}"]
}


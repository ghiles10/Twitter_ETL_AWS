provider "aws" {
  region     = "us-east-1"
  access_key = "PUT YOUR OWN"
  secret_key = "PUT YOUR OWN"
} 

# Data Lake s3 bucket 
resource "aws_s3_bucket" "data_lake" {
  bucket  = var.bucket_name
  force_destroy = true
} 

# IAM role for Redshift to be able to use S3 

resource "aws_iam_role" "redshift_iam_role" { 

  name = "redshift_iam_role" 
  assume_role_policy = jsonencode({
    
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole"
            ],
            "Principal": {
                "Service": [
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

  cluster_identifier = ${var.dwh_cluster_identifier}
  database_name      = "mydb"
  master_username    = "exampleuser"
  master_password    = "Mustbe8characters"
  node_type          = "dc1.large"
  cluster_type       = "single-node"
}

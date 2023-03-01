variable "aws_region" {
  description = "Aws region"
  type        = string
  default     = "eu-west-3"
}

variable "bucket_backend" {
  description = "Bucket name"
  type        = string
  default     = "terraform-backend-etl-ghiles"
}

variable "bucket_name" {
  description = "Bucket name"
  type        = string
  default     = "data-lake-batch-etl"
}

variable "vpc_cidr" {
  type    = string
  default = ""
}

variable "dwh_port" {
  type    = string
  default = "5439"
}

variable "dwh_cluster_identifier" {
  type    = string
  default = "dwhcluster"
}

variable "dwh_db" {

  type    = string
  default = "mydwhdb"
}

variable "master_username" {
  type    = string
  default = "mydwhuser"

}

variable "master_password" {

  type    = string
  default = "Mydwhpassword1!"
}


variable "redshift_iam_role" {
  type    = string
  default = "mydwhrole_tf"
}

variable "dwh_node_type" {
  type    = string
  default = "dc2.large"
}

variable "dwh_num_nodes" {
  type    = string
  default = "1"
}

variable "dwh_cluster_type" {
  type    = string
  default = "single-node"
}
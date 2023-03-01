variable "aws_region" {
    description = "Aws region"
    type = string
    defaut = "eu-west-3" 
}

variable "bucket_name" { 
    description = "Bucket name"
    type = string
    defaut = "data-lake-batch-etl" 
}

variable "vpc_cidr" {   
    type  = string 
    defaut = ""
}

variable "dwh_port" {   
    type  = string 
    defaut = "5439"
}

variable dwh_cluster_identifier {   
    type  = string 
    defaut = "dwhcluster"
}


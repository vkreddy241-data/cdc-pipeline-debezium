variable "aws_region"         { default = "us-east-1" }
variable "vpc_id"             { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "db_password"        { type = string; sensitive = true }

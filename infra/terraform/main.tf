terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket = "vkreddy-tf-state"
    key    = "cdc-pipeline/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" { region = var.aws_region }

locals {
  tags = { Project = "cdc-pipeline-debezium", Owner = "vikas-reddy", ManagedBy = "terraform" }
}

# ---------------------------------------------------------------------------
# RDS PostgreSQL (CDC source)
# ---------------------------------------------------------------------------
resource "aws_db_instance" "postgres_source" {
  identifier             = "cdc-postgres-source"
  engine                 = "postgres"
  engine_version         = "15.5"
  instance_class         = "db.t3.medium"
  allocated_storage      = 100
  storage_type           = "gp3"
  db_name                = "claims_db"
  username               = "debezium"
  password               = var.db_password
  parameter_group_name   = aws_db_parameter_group.pg_logical.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  skip_final_snapshot    = false
  backup_retention_period = 7
  tags                   = local.tags
}

resource "aws_db_parameter_group" "pg_logical" {
  name   = "pg15-logical-replication"
  family = "postgres15"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name  = "max_replication_slots"
    value = "10"
  }

  parameter {
    name  = "max_wal_senders"
    value = "10"
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# MSK (Kafka) — same as stock pipeline, reuse pattern
# ---------------------------------------------------------------------------
resource "aws_msk_cluster" "cdc_kafka" {
  cluster_name           = "cdc-pipeline-kafka"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.kafka_sg.id]
    storage_info {
      ebs_storage_info { volume_size = 200 }
    }
  }

  encryption_info {
    encryption_in_transit { client_broker = "TLS" }
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# ECS Fargate — Debezium Kafka Connect
# ---------------------------------------------------------------------------
resource "aws_ecs_cluster" "debezium" {
  name = "debezium-connect-cluster"
  tags = local.tags
}

resource "aws_ecs_task_definition" "debezium" {
  family                   = "debezium-connect"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"
  memory                   = "4096"
  execution_role_arn       = aws_iam_role.ecs_exec.arn

  container_definitions = jsonencode([{
    name  = "debezium-connect"
    image = "debezium/connect:2.5"
    environment = [
      { name = "BOOTSTRAP_SERVERS",      value = aws_msk_cluster.cdc_kafka.bootstrap_brokers_tls },
      { name = "GROUP_ID",               value = "debezium-fargate" },
      { name = "CONFIG_STORAGE_TOPIC",   value = "debezium_configs" },
      { name = "OFFSET_STORAGE_TOPIC",   value = "debezium_offsets" },
      { name = "STATUS_STORAGE_TOPIC",   value = "debezium_statuses" },
    ]
    portMappings = [{ containerPort = 8083, protocol = "tcp" }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"  = "/ecs/debezium-connect"
        "awslogs-region" = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])

  tags = local.tags
}

resource "aws_security_group" "rds_sg" {
  name   = "cdc-rds-sg"
  vpc_id = var.vpc_id
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  egress { from_port = 0; to_port = 0; protocol = "-1"; cidr_blocks = ["0.0.0.0/0"] }
  tags = local.tags
}

resource "aws_security_group" "kafka_sg" {
  name   = "cdc-kafka-sg"
  vpc_id = var.vpc_id
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  egress { from_port = 0; to_port = 0; protocol = "-1"; cidr_blocks = ["0.0.0.0/0"] }
  tags = local.tags
}

resource "aws_db_subnet_group" "main" {
  name       = "cdc-db-subnet-group"
  subnet_ids = var.private_subnet_ids
  tags       = local.tags
}

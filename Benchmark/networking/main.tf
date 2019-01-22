resource "aws_vpc" "cluster_net" {
  cidr_block = "${var.cidr}"
}

resource "aws_internet_gateway" "cluster_net" {
  vpc_id = "${aws_vpc.cluster_net.id}"
}

resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.cluster_net.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.cluster_net.id}"
}

resource "aws_subnet" "cluster_net" {
  vpc_id                  = "${aws_vpc.cluster_net.id}"
  cidr_block              = "${aws_vpc.cluster_net.cidr_block}"
  map_public_ip_on_launch = true
}

resource "aws_security_group" "allow-ssh-flink-egress" {
  vpc_id = "${aws_vpc.cluster_net.id}"

  # Allow all inbound traffic inside VPC
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"

    cidr_blocks = [
      "${var.cidr}",
    ]
  }

  # Allow everyone to view FLink dashboard
  ingress {
    from_port = 8081
    to_port   = 8081
    protocol  = "tcp"

    cidr_blocks = [
      "0.0.0.0/0",
    ]
  }

  # Allow SSH in
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow everything out
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

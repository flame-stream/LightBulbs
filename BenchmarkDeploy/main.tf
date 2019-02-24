variable "access_key" {}
variable "secret_key" {}

variable "cluster_size" {
  default = 3
}

provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region     = "eu-west-3"
}

module "networking" {
  source = "./networking"
  cidr   = "10.0.0.0/24"
}

resource "aws_instance" "worker" {
  count         = "${var.cluster_size}"
  ami           = "ami-08182c55a1c188dee"
  instance_type = "t2.micro"
  key_name      = "admin-paris"
  subnet_id     = "${module.networking.subnet_id}"

  vpc_security_group_ids = [
    "${module.networking.security_group_id}",
  ]

  associate_public_ip_address = true
}

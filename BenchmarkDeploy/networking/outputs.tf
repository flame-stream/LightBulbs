output "subnet_id" {
  value = "${aws_subnet.cluster_net.id}"
}

output "security_group_id" {
  value = "${aws_security_group.allow-ssh-flink-egress.id}"
}

output "public_ips" {
  value = "${join(", ", aws_instance.node.*.public_ip)}"
}

output "private_ips" {
  value = "${join(", ", aws_instance.node.*.private_ip)}"
}

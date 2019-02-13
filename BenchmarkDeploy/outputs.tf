output "public_ips" {
  value = "${join(", ", aws_instance.worker.*.public_ip)}"
}

output "private_ips" {
  value = "${join(", ", aws_instance.worker.*.private_ip)}"
}

output "public_ip" {
  value = aws_instance.bench.public_ip
}

output "ssh" {
  value = "ssh -A ubuntu@${aws_instance.bench.public_ip}"
}

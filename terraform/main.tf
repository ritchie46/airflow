data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name = "name"
    values = [
      "ubuntu/images/hvm-ssd/ubuntu-*-18.04-amd64-server-*"]
  }

  filter {
    name = "virtualization-type"
    values = [
      "hvm"]
  }

  owners = [
    "099720109477"]
  # Canonical
}

resource "aws_instance" "web" {
  ami = "${data.aws_ami.ubuntu.id}"
  instance_type = "t2.medium"
  security_groups = [
    "${aws_security_group.sg.name}"]
  key_name = "enx-ec2"
  iam_instance_profile = "${aws_iam_instance_profile.instance-profile.name}"

  provisioner "remote-exec" {
    script = "./bootstrap.sh"

    connection {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("~/.ssh/aws-enx/enx-ec2.pem")}"
    }
  }
  # copy project
  provisioner "file" {
    source = "../../docker-airflow"
    destination = "/var/app"
    connection {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("~/.ssh/aws-enx/enx-ec2.pem")}"
    }
  }
  # start application
  provisioner "remote-exec" {
    inline = [
      "chmod +x /var/app/docker-airflow/script/entrypoint.sh",
      "sudo docker-compose -f /var/app/docker-airflow/local-docker-compose-LocalExecutor.yml up -d"
    ]
    connection {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("~/.ssh/aws-enx/enx-ec2.pem")}"
    }
  }

  provisioner "local-exec" {
    command = "echo ${aws_instance.web.public_ip} > ../ip_address.txt"
  }

  tags = {
    Name = "${var.tag}"
  }
}


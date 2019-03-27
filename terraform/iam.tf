# https://medium.com/@kulasangar91/creating-and-attaching-an-aws-iam-role-with-a-policy-to-an-ec2-instance-using-terraform-scripts-aa85f3e6dfff

resource "aws_iam_role" "ec2-resource-access-role" {
  name               = "${var.tag}-ec2-resource-access-role"
  assume_role_policy = "${file("assumerolepolicy.json")}"
}

resource "aws_iam_policy" "policy" {
  name        = "enx-datascience-airflow-policy"
  policy      = "${file("iampolicy.json")}"
}

resource "aws_iam_policy_attachment" "attach-iam" {
  name = "${var.tag}-attach-iam-policy"
  roles = ["${aws_iam_role.ec2-resource-access-role.name}"]
  policy_arn = "${aws_iam_policy.policy.arn}"
}

resource "aws_iam_instance_profile" "instance-profile" {
  name  = "instance-profile"
  role = "${aws_iam_role.ec2-resource-access-role.name}"
}
resource "aws_s3_bucket" "bucket" {
  bucket = "hymaia-spark-handson"

  tags = local.tags
}

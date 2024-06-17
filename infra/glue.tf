resource "aws_glue_job" "exo2_clean_job" {
  name     = "exo2_clean_job"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://${aws_s3_bucket.bucket.bucket}/spark-jobs/exo2_glue_job.py"
  }

  glue_version = "4.0"
  number_of_workers = 2
  worker_type = "Standard"

  default_arguments = {
    "--python-modules-installer-option" = "--upgrade"
    "--additional-python-modules"       = "s3://${aws_s3_bucket.bucket.bucket}/wheel/spark_handson-0.1.0-py3-none-any.whl"
    "--job-language"                    = "python"
    "--CLIENT_FILE"                     = "s3://${aws_s3_bucket.bucket.bucket}/spark_formation/data/clients_bdd.csv"
    "--CITY_FILE"                       = "s3://${aws_s3_bucket.bucket.bucket}/spark_formation/data/city_zipcode.csv"
    "--OUTPUT_FILE"                     = "s3://${aws_s3_bucket.bucket.bucket}/spark_formation/data/output"
  }

  tags = local.tags
}

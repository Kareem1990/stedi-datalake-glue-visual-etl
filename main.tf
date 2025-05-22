resource "aws_s3_bucket" "stedi_datalake" {
  bucket = "stedi-datalake-terraform"
  force_destroy = true

  tags = {
    Project = "STEDI"
  }
}

# إنشاء مجلدات داخل الباكت
resource "aws_s3_object" "folders" {
  for_each = toset([
    "customer_landing/",
    "accelerometer_landing/",
    "step_trainer_landing/"
  ])
  bucket  = aws_s3_bucket.stedi_datalake.id
  key     = each.value
  content = ""
}

# IAM Role لـ AWS Glue
resource "aws_iam_role" "glue_role" {
  name = "stedi-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

# Attach policies to allow Glue to access S3
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_basic" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Glue Database
resource "aws_glue_catalog_database" "stedi_db" {
  name = "stedi_lake"
}


#########################################
#         IAM Policy for voclabs Role   #
#########################################

resource "aws_glue_catalog_policy" "allow_voclabs_glue_access" {
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          AWS = "arn:aws:iam::446509598474:role/voclabs"
        },
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables"
        ],
        Resource = "*"
      }
    ]
  })
}

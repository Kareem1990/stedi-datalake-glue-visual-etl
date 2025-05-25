# Create S3 Bucket to hold STEDI data lake
resource "aws_s3_bucket" "stedi_datalake" {
  bucket         = "stedi-datalake-terraform"
  force_destroy  = true

  tags = {
    Project = "STEDI"
  }
}

# Create folders inside the S3 bucket for different landing zones
resource "aws_s3_object" "folders" {
  for_each = toset([
    "customer_landing/",
    "accelerometer_landing/",
    "step_trainer_landing/"
  ])
  bucket  = aws_s3_bucket.stedi_datalake.id
  key     = each.value
  content = ""  # Creates empty folder by uploading an empty object with "/" suffix
}

# Create IAM Role for AWS Glue with trust policy allowing Glue to assume it
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

# Attach AmazonS3FullAccess policy to Glue role for full access to S3
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Attach AWSGlueServiceRole policy to allow Glue basic permissions
resource "aws_iam_role_policy_attachment" "glue_basic" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Create a Glue database to organize tables
resource "aws_glue_catalog_database" "stedi_db" {
  name = "stedi_lake"
}

# Custom policy to allow reading from the Glue Data Catalog
resource "aws_iam_policy" "glue_catalog_access" {
  name        = "GlueCatalogReadAccess"
  description = "Allow read access to AWS Glue Data Catalog and required S3 access for preview"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      }
    ]
  })
}
# Create an IAM policy attachment to allow Glue to access the Data Catalog 

# Attach the custom Glue Data Catalog policy to the correct IAM role
resource "aws_iam_policy_attachment" "voclabs_glue_catalog_attach" {
  name       = "attach-voclabs-glue-access"
  roles      = [aws_iam_role.glue_role.name]  # ✅ Corrected to use the actual role created
  policy_arn = aws_iam_policy.glue_catalog_access.arn

  # 👇 Make sure this attachment waits for both the role and the policy to exist first
  depends_on = [
    aws_iam_role.glue_role,
    aws_iam_policy.glue_catalog_access
  ]
}
